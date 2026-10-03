package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/store/db"
)

const (
	dialogFilterPinned int16 = iota + 1
	dialogFilterIncluded
	dialogFilterExcluded
	dialogFilterMaxPeers    = 100
	dialogFilterMaxEntities = 12
)

// DialogFilterPeer is an owner-authorized peer reference stored in a private
// dialog filter. The type is required because peer identifiers share no space.
type DialogFilterPeer struct {
	Type PeerType
	ID   int64
}

// DialogFilterEntity is a custom-emoji entity in UTF-16 code units.
type DialogFilterEntity struct {
	Offset     int
	Length     int
	DocumentID int64
}

// DialogFilter is one ordinary private folder and its owner-scoped settings.
type DialogFilter struct {
	ID              int
	Title           string
	Entities        []DialogFilterEntity
	Emoticon        string
	Color           *int
	Contacts        bool
	NonContacts     bool
	Groups          bool
	Broadcasts      bool
	Bots            bool
	ExcludeMuted    bool
	ExcludeRead     bool
	ExcludeArchived bool
	TitleNoanimate  bool
	PinnedPeers     []DialogFilterPeer
	IncludePeers    []DialogFilterPeer
	ExcludePeers    []DialogFilterPeer
}

// DialogFilterSnapshot is a consistent read of one owner's folders and order.
type DialogFilterSnapshot struct {
	Filters   []DialogFilter
	Order     []int
	ChangedAt *time.Time
}

// ErrDialogFilterLimit reports that the owner already has the maximum number
// of custom folders.
var ErrDialogFilterLimit = errors.New("dialog filter limit reached")

// DialogFilters returns a repeatable-read snapshot of the owner's folders and
// order without changing folder state.
func (s *Store) DialogFilters(ctx context.Context, ownerID int64) (DialogFilterSnapshot, error) {
	return s.readDialogFilters(ctx, ownerID)
}

// DialogFilterDefinitions returns stored folders without seeding defaults.
// Suggested folders use this read so asking for recommendations does not
// initialize an account's folder state.
func (s *Store) DialogFilterDefinitions(ctx context.Context, ownerID int64) ([]DialogFilter, error) {
	snapshot, err := s.readDialogFilters(ctx, ownerID)
	if err != nil {
		return nil, err
	}
	return snapshot.Filters, nil
}

// DefaultDialogFilters returns the four private, peer-free folder definitions
// created for accounts on their first authenticated folder read.
func DefaultDialogFilters() []DialogFilter {
	return []DialogFilter{
		{Title: "Personal", Contacts: true, NonContacts: true, Bots: true},
		{Title: "Channels", Broadcasts: true},
		{Title: "Groups", Groups: true},
		{Title: "Unread", Contacts: true, NonContacts: true, Groups: true, Broadcasts: true, Bots: true, ExcludeRead: true},
	}
}

// DialogFilterDefaultsSeeded reports whether the owner has completed the
// one-time default initialization. A missing state row is unseeded.
func (s *Store) DialogFilterDefaultsSeeded(ctx context.Context, ownerID int64) (bool, error) {
	seeded, err := s.q.DialogFilterDefaultsSeeded(ctx, ownerID)
	if err != nil {
		return false, fmt.Errorf("read dialog filter defaults marker: %w", err)
	}
	return seeded, nil
}

// SeedDefaultDialogFilters atomically appends missing default titles once.
// Folder reads call this only after authenticating the request.
func (s *Store) SeedDefaultDialogFilters(ctx context.Context, ownerID int64) (bool, error) {
	seeded, err := s.DialogFilterDefaultsSeeded(ctx, ownerID)
	if err != nil {
		return false, err
	}
	if seeded {
		return false, nil
	}

	tx, qtx, state, err := s.beginDialogFilterMutation(ctx, ownerID)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }() //nolint:errcheck // no-op after commit
	if state.DefaultsSeededAt.Valid {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit default dialog filter seed check: %w", err)
		}
		return false, nil
	}

	rows, err := qtx.ListDialogFilters(ctx, ownerID)
	if err != nil {
		return false, fmt.Errorf("list existing folders before default seed: %w", err)
	}
	usedIDs := make(map[int]struct{}, len(rows))
	existingTitles := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		usedIDs[int(row.FilterID)] = struct{}{}
		existingTitles[dialogFilterTitleKey(row.Title)] = struct{}{}
	}
	order := NormalizeDialogFilterOrder(dialogFilterIDs(state.OrderIds), dialogFilterRowIDs(rows))
	addedIDs := make([]int, 0, 4)
	for _, definition := range DefaultDialogFilters() {
		if len(rows)+len(addedIDs) >= 10 {
			break
		}
		if _, exists := existingTitles[dialogFilterTitleKey(definition.Title)]; exists {
			continue
		}
		id := nextAvailableDialogFilterID(usedIDs)
		if id == 0 {
			break
		}
		filterID, err := dialogFilterSmallint(id)
		if err != nil {
			return false, rollbackDialogFilterMutation(tx, fmt.Errorf("default dialog filter id %d: %w", id, err))
		}
		if err := qtx.InsertDialogFilter(ctx, db.InsertDialogFilterParams{
			OwnerID: ownerID, FilterID: filterID, Title: definition.Title,
			Contacts: definition.Contacts, NonContacts: definition.NonContacts,
			Groups: definition.Groups, Broadcasts: definition.Broadcasts,
			Bots: definition.Bots, ExcludeMuted: definition.ExcludeMuted,
			ExcludeRead: definition.ExcludeRead, ExcludeArchived: definition.ExcludeArchived,
			TitleNoanimate: definition.TitleNoanimate,
		}); err != nil {
			return false, rollbackDialogFilterMutation(tx, fmt.Errorf("insert default dialog filter %q: %w", definition.Title, err))
		}
		usedIDs[id] = struct{}{}
		existingTitles[dialogFilterTitleKey(definition.Title)] = struct{}{}
		addedIDs = append(addedIDs, id)
	}
	order = append(order, addedIDs...)
	if err := qtx.MarkDialogFilterDefaultsSeeded(ctx, ownerID); err != nil {
		return false, rollbackDialogFilterMutation(tx, fmt.Errorf("mark default dialog filters seeded: %w", err))
	}
	if err := commitDialogFilterMutation(ctx, tx, qtx, ownerID, order); err != nil {
		return false, rollbackDialogFilterMutation(tx, err)
	}
	return true, nil
}

func dialogFilterTitleKey(title string) string {
	return strings.ToLower(strings.TrimSpace(title))
}

func nextAvailableDialogFilterID(used map[int]struct{}) int {
	for id := 2; id <= 255; id++ {
		if _, exists := used[id]; !exists {
			return id
		}
	}
	return 0
}

// readDialogFilters returns a repeatable-read snapshot without changing the
// owner's folder state.
func (s *Store) readDialogFilters(ctx context.Context, ownerID int64) (DialogFilterSnapshot, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return DialogFilterSnapshot{}, fmt.Errorf("begin dialog filters read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	snapshot := DialogFilterSnapshot{}
	state, err := qtx.DialogFilterState(ctx, ownerID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		snapshot.Order = []int{0}
	case err != nil:
		return DialogFilterSnapshot{}, fmt.Errorf("read dialog filter state: %w", err)
	default:
		snapshot.Order = dialogFilterIDs(state.OrderIds)
		if state.ChangedAt.Valid {
			changedAt := state.ChangedAt.Time
			snapshot.ChangedAt = &changedAt
		}
	}

	rows, err := qtx.ListDialogFilters(ctx, ownerID)
	if err != nil {
		return DialogFilterSnapshot{}, fmt.Errorf("list dialog filters: %w", err)
	}
	index := make(map[int16]int, len(rows))
	snapshot.Filters = make([]DialogFilter, len(rows))
	for i, row := range rows {
		filter := DialogFilter{
			ID:              int(row.FilterID),
			Title:           row.Title,
			Emoticon:        row.Emoticon,
			Contacts:        row.Contacts,
			NonContacts:     row.NonContacts,
			Groups:          row.Groups,
			Broadcasts:      row.Broadcasts,
			Bots:            row.Bots,
			ExcludeMuted:    row.ExcludeMuted,
			ExcludeRead:     row.ExcludeRead,
			ExcludeArchived: row.ExcludeArchived,
			TitleNoanimate:  row.TitleNoanimate,
		}
		if row.Color != nil {
			color := int(*row.Color)
			filter.Color = &color
		}
		snapshot.Filters[i] = filter
		index[row.FilterID] = i
	}

	peerRows, err := qtx.ListDialogFilterPeers(ctx, ownerID)
	if err != nil {
		return DialogFilterSnapshot{}, fmt.Errorf("list dialog filter peers: %w", err)
	}
	for _, row := range peerRows {
		i, ok := index[row.FilterID]
		if !ok {
			continue
		}
		peer := DialogFilterPeer{Type: PeerType(row.PeerType), ID: row.PeerID}
		filter := &snapshot.Filters[i]
		switch row.ListType {
		case dialogFilterPinned:
			filter.PinnedPeers = append(filter.PinnedPeers, peer)
		case dialogFilterIncluded:
			filter.IncludePeers = append(filter.IncludePeers, peer)
		case dialogFilterExcluded:
			filter.ExcludePeers = append(filter.ExcludePeers, peer)
		}
	}

	entityRows, err := qtx.ListDialogFilterEntities(ctx, ownerID)
	if err != nil {
		return DialogFilterSnapshot{}, fmt.Errorf("list dialog filter entities: %w", err)
	}
	for _, row := range entityRows {
		i, ok := index[row.FilterID]
		if !ok {
			continue
		}
		snapshot.Filters[i].Entities = append(snapshot.Filters[i].Entities, DialogFilterEntity{
			Offset:     int(row.EntityOffset),
			Length:     int(row.Length),
			DocumentID: row.DocumentID,
		})
	}

	ids := make([]int, len(snapshot.Filters))
	for i := range snapshot.Filters {
		ids[i] = snapshot.Filters[i].ID
	}
	snapshot.Order = NormalizeDialogFilterOrder(snapshot.Order, ids)
	if err := tx.Commit(ctx); err != nil {
		return DialogFilterSnapshot{}, fmt.Errorf("commit dialog filters read: %w", err)
	}
	return snapshot, nil
}

// NormalizeDialogFilterOrder returns one complete order with All chats at most
// once, retaining the first requested occurrence and appending existing ids.
func NormalizeDialogFilterOrder(requested, existing []int) []int {
	valid := make(map[int]struct{}, len(existing)+1)
	valid[0] = struct{}{}
	for _, id := range existing {
		if id >= 2 && id <= 255 {
			valid[id] = struct{}{}
		}
	}
	seen := make(map[int]struct{}, len(valid))
	out := make([]int, 0, len(valid))
	appendValid := func(id int) {
		if _, ok := valid[id]; !ok {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, id := range requested {
		appendValid(id)
	}
	for _, id := range existing {
		appendValid(id)
	}
	appendValid(0)
	return out
}

// AccessibleDialogFilterPeers returns true for peers the owner can currently
// reference. Each peer type is checked in one bounded query.
func (s *Store) AccessibleDialogFilterPeers(ctx context.Context, ownerID int64, peers []DialogFilterPeer, now time.Time) (map[DialogFilterPeer]bool, error) {
	users := make([]int64, 0, len(peers))
	chats := make([]int64, 0, len(peers))
	channels := make([]int64, 0, len(peers))
	seenUsers := make(map[int64]struct{})
	seenChats := make(map[int64]struct{})
	seenChannels := make(map[int64]struct{})
	out := make(map[DialogFilterPeer]bool, len(peers))
	for _, peer := range peers {
		out[peer] = false
		switch peer.Type {
		case PeerTypeUser:
			if _, ok := seenUsers[peer.ID]; !ok {
				seenUsers[peer.ID] = struct{}{}
				users = append(users, peer.ID)
			}
		case PeerTypeChat:
			if _, ok := seenChats[peer.ID]; !ok {
				seenChats[peer.ID] = struct{}{}
				chats = append(chats, peer.ID)
			}
		case PeerTypeChannel:
			if _, ok := seenChannels[peer.ID]; !ok {
				seenChannels[peer.ID] = struct{}{}
				channels = append(channels, peer.ID)
			}
		}
	}

	if len(users) > 0 {
		rows, err := s.q.UsersByID(ctx, users)
		if err != nil {
			return nil, fmt.Errorf("authorize dialog filter users: %w", err)
		}
		for _, row := range rows {
			out[DialogFilterPeer{Type: PeerTypeUser, ID: row.ID}] = true
		}
	}
	if len(chats) > 0 {
		rows, err := s.q.DialogFilterChatMemberships(ctx, db.DialogFilterChatMembershipsParams{
			UserID: ownerID, ChatIds: chats,
		})
		if err != nil {
			return nil, fmt.Errorf("authorize dialog filter groups: %w", err)
		}
		for _, id := range rows {
			out[DialogFilterPeer{Type: PeerTypeChat, ID: id}] = true
		}
	}
	if len(channels) > 0 {
		rows, err := s.q.DialogFilterChannelMemberships(ctx, db.DialogFilterChannelMembershipsParams{
			UserID: ownerID, ChannelIds: channels,
		})
		if err != nil {
			return nil, fmt.Errorf("authorize dialog filter channels: %w", err)
		}
		for _, row := range rows {
			member := channelMemberFromRow(row)
			if member.Banned(now) {
				continue
			}
			out[DialogFilterPeer{Type: PeerTypeChannel, ID: row.ChannelID}] = true
		}
	}
	return out, nil
}

// SaveDialogFilter atomically upserts a folder, replaces its lists and entities,
// normalizes the order, commits its database-clock marker, and emits one private
// owner invalidation. A single owner state row serializes all folder mutations.
func (s *Store) SaveDialogFilter(ctx context.Context, ownerID int64, filter DialogFilter) error {
	if filter.ID < 2 || filter.ID > 255 {
		return errors.New("dialog filter id outside supported range")
	}
	filterID, err := dialogFilterSmallint(filter.ID)
	if err != nil {
		return err
	}
	tx, qtx, state, err := s.beginDialogFilterMutation(ctx, ownerID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }() //nolint:errcheck // no-op after commit

	exists, err := qtx.DialogFilterExists(ctx, db.DialogFilterExistsParams{OwnerID: ownerID, FilterID: filterID})
	if err != nil {
		return fmt.Errorf("check dialog filter: %w", err)
	}
	if !exists {
		count, err := qtx.DialogFilterFolderCount(ctx, ownerID)
		if err != nil {
			return fmt.Errorf("count dialog filters: %w", err)
		}
		if count >= 10 {
			return ErrDialogFilterLimit
		}
	}

	var color *int16
	if filter.Color != nil {
		if *filter.Color < 0 || *filter.Color > 6 {
			return errors.New("dialog filter color outside supported range")
		}
		value, err := dialogFilterSmallint(*filter.Color)
		if err != nil {
			return err
		}
		color = &value
	}
	if err := qtx.UpsertDialogFilter(ctx, db.UpsertDialogFilterParams{
		OwnerID:         ownerID,
		FilterID:        filterID,
		Title:           filter.Title,
		Emoticon:        filter.Emoticon,
		Color:           color,
		Contacts:        filter.Contacts,
		NonContacts:     filter.NonContacts,
		Groups:          filter.Groups,
		Broadcasts:      filter.Broadcasts,
		Bots:            filter.Bots,
		ExcludeMuted:    filter.ExcludeMuted,
		ExcludeRead:     filter.ExcludeRead,
		ExcludeArchived: filter.ExcludeArchived,
		TitleNoanimate:  filter.TitleNoanimate,
	}); err != nil {
		return fmt.Errorf("save dialog filter: %w", err)
	}
	if err := replaceDialogFilterLists(ctx, qtx, ownerID, filter); err != nil {
		return err
	}
	if err := replaceDialogFilterEntities(ctx, qtx, ownerID, filter); err != nil {
		return err
	}

	rows, err := qtx.ListDialogFilters(ctx, ownerID)
	if err != nil {
		return fmt.Errorf("read dialog filter order: %w", err)
	}
	ids := dialogFilterRowIDs(rows)
	current := NormalizeDialogFilterOrder(dialogFilterIDs(state.OrderIds), ids)
	if !slices.Contains(current, filter.ID) {
		current = append(current, filter.ID)
	}
	return commitDialogFilterMutation(ctx, tx, qtx, ownerID, current)
}

// DeleteDialogFilter deletes a folder if it exists. Deleting an absent id is a
// successful no-op and does not change the committed marker or notify owner
// sessions.
func (s *Store) DeleteDialogFilter(ctx context.Context, ownerID int64, filterID int) (bool, error) {
	if filterID < 2 || filterID > 255 {
		return false, errors.New("dialog filter id outside supported range")
	}
	tx, qtx, state, err := s.beginDialogFilterMutation(ctx, ownerID)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }() //nolint:errcheck // no-op after commit

	deleted, err := qtx.DeleteDialogFilter(ctx, db.DeleteDialogFilterParams{OwnerID: ownerID, FilterID: int16(filterID)})
	if err != nil {
		return false, fmt.Errorf("delete dialog filter: %w", err)
	}
	requested := slices.DeleteFunc(dialogFilterIDs(state.OrderIds), func(id int) bool { return id == filterID })
	rows, err := qtx.ListDialogFilters(ctx, ownerID)
	if err != nil {
		return false, fmt.Errorf("read dialog filter order after delete: %w", err)
	}
	order := NormalizeDialogFilterOrder(requested, dialogFilterRowIDs(rows))
	orderChanged := !slices.Equal(order, dialogFilterIDs(state.OrderIds))
	if deleted == 0 && !orderChanged {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit absent dialog filter delete: %w", err)
		}
		return false, nil
	}
	if err := commitDialogFilterMutation(ctx, tx, qtx, ownerID, order); err != nil {
		return false, err
	}
	return true, nil
}

// UpdateDialogFilterOrder stores a complete, duplicate-free permutation of the
// existing folders and All chats. Unknown ids are ignored.
func (s *Store) UpdateDialogFilterOrder(ctx context.Context, ownerID int64, requested []int) (bool, error) {
	tx, qtx, state, err := s.beginDialogFilterMutation(ctx, ownerID)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }() //nolint:errcheck // no-op after commit

	rows, err := qtx.ListDialogFilters(ctx, ownerID)
	if err != nil {
		return false, fmt.Errorf("read dialog filters for reorder: %w", err)
	}
	ids := dialogFilterRowIDs(rows)
	previous := NormalizeDialogFilterOrder(dialogFilterIDs(state.OrderIds), ids)
	order := NormalizeDialogFilterOrder(requested, previous)
	if slices.Equal(order, previous) {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit unchanged dialog filter order: %w", err)
		}
		return false, nil
	}
	if err := commitDialogFilterMutation(ctx, tx, qtx, ownerID, order); err != nil {
		return false, err
	}
	return true, nil
}

// DialogFilterChangeAt returns the durable database-clock time of the owner's
// last committed folder mutation. It survives the last folder's deletion.
func (s *Store) DialogFilterChangeAt(ctx context.Context, ownerID int64) (time.Time, bool, error) {
	value, err := s.q.DialogFilterChangeAt(ctx, ownerID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return time.Time{}, false, nil
	case err != nil:
		return time.Time{}, false, fmt.Errorf("read dialog filter change marker: %w", err)
	case !value.Valid:
		return time.Time{}, false, nil
	default:
		return value.Time, true, nil
	}
}

func (s *Store) beginDialogFilterMutation(ctx context.Context, ownerID int64) (pgx.Tx, *db.Queries, db.UserDialogFilterState, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, nil, db.UserDialogFilterState{}, fmt.Errorf("begin dialog filter mutation: %w", err)
	}
	qtx := s.q.WithTx(tx)
	if err := qtx.EnsureDialogFilterState(ctx, ownerID); err != nil {
		cause := fmt.Errorf("ensure dialog filter state: %w", err)
		return nil, nil, db.UserDialogFilterState{}, rollbackDialogFilterMutation(tx, cause)
	}
	state, err := qtx.DialogFilterStateForUpdate(ctx, ownerID)
	if err != nil {
		cause := fmt.Errorf("lock dialog filter state: %w", err)
		return nil, nil, db.UserDialogFilterState{}, rollbackDialogFilterMutation(tx, cause)
	}
	if _, err := qtx.LockUpdateState(ctx, ownerID); err != nil {
		cause := fmt.Errorf("lock owner update state: %w", err)
		return nil, nil, db.UserDialogFilterState{}, rollbackDialogFilterMutation(tx, cause)
	}
	return tx, qtx, state, nil
}

func replaceDialogFilterLists(ctx context.Context, qtx *db.Queries, ownerID int64, filter DialogFilter) error {
	filterID, err := dialogFilterSmallint(filter.ID)
	if err != nil {
		return err
	}
	params := db.DeleteDialogFilterPeersParams{OwnerID: ownerID, FilterID: filterID}
	if err := qtx.DeleteDialogFilterPeers(ctx, params); err != nil {
		return fmt.Errorf("replace dialog filter peers: %w", err)
	}
	for listType, peers := range map[int16][]DialogFilterPeer{
		dialogFilterPinned:   filter.PinnedPeers,
		dialogFilterIncluded: filter.IncludePeers,
		dialogFilterExcluded: filter.ExcludePeers,
	} {
		if len(peers) > dialogFilterMaxPeers {
			return errors.New("dialog filter peer list exceeds supported limit")
		}
		for position, peer := range peers {
			peerPosition, err := dialogFilterSmallint(position)
			if err != nil {
				return err
			}
			if err := qtx.InsertDialogFilterPeer(ctx, db.InsertDialogFilterPeerParams{
				OwnerID: ownerID, FilterID: filterID, ListType: listType,
				PeerType: int16(peer.Type), PeerID: peer.ID, PeerPosition: peerPosition,
			}); err != nil {
				return fmt.Errorf("insert dialog filter peer: %w", err)
			}
		}
	}
	return nil
}

func replaceDialogFilterEntities(ctx context.Context, qtx *db.Queries, ownerID int64, filter DialogFilter) error {
	filterID, err := dialogFilterSmallint(filter.ID)
	if err != nil {
		return err
	}
	if len(filter.Entities) > dialogFilterMaxEntities {
		return errors.New("dialog filter entity list exceeds supported limit")
	}
	params := db.DeleteDialogFilterEntitiesParams{OwnerID: ownerID, FilterID: filterID}
	if err := qtx.DeleteDialogFilterEntities(ctx, params); err != nil {
		return fmt.Errorf("replace dialog filter entities: %w", err)
	}
	for position, entity := range filter.Entities {
		entityPosition, err := dialogFilterSmallint(position)
		if err != nil {
			return err
		}
		entityOffset, err := dialogFilterInt32(entity.Offset)
		if err != nil {
			return err
		}
		length, err := dialogFilterInt32(entity.Length)
		if err != nil {
			return err
		}
		if err := qtx.InsertDialogFilterEntity(ctx, db.InsertDialogFilterEntityParams{
			OwnerID: ownerID, FilterID: filterID, EntityPosition: entityPosition,
			EntityOffset: entityOffset, Length: length, DocumentID: entity.DocumentID,
		}); err != nil {
			return fmt.Errorf("insert dialog filter entity: %w", err)
		}
	}
	return nil
}

func commitDialogFilterMutation(ctx context.Context, tx pgx.Tx, qtx *db.Queries, ownerID int64, order []int) error {
	if err := qtx.NotifyDialogFilterMutation(ctx, strconv.FormatInt(ownerID, 10)); err != nil {
		return fmt.Errorf("notify dialog filter mutation: %w", err)
	}
	orderIDs, err := dialogFilterOrderIDs(order)
	if err != nil {
		return fmt.Errorf("encode dialog filter order: %w", err)
	}
	if err := qtx.CommitDialogFilterMutation(ctx, db.CommitDialogFilterMutationParams{
		OwnerID: ownerID, OrderIds: orderIDs,
	}); err != nil {
		return fmt.Errorf("commit dialog filter marker: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dialog filter mutation: %w", err)
	}
	return nil
}

func dialogFilterRowIDs(rows []db.UserDialogFilter) []int {
	ids := make([]int, len(rows))
	for i, row := range rows {
		ids[i] = int(row.FilterID)
	}
	return ids
}

func dialogFilterIDs(ids []int16) []int {
	out := make([]int, len(ids))
	for i, id := range ids {
		out[i] = int(id)
	}
	return out
}

func dialogFilterOrderIDs(ids []int) ([]int16, error) {
	out := make([]int16, len(ids))
	for i, id := range ids {
		value, err := dialogFilterSmallint(id)
		if err != nil {
			return nil, fmt.Errorf("folder id %d: %w", id, err)
		}
		out[i] = value
	}
	return out, nil
}

func dialogFilterSmallint(value int) (int16, error) {
	if value < math.MinInt16 || value > math.MaxInt16 {
		return 0, errors.New("dialog filter value outside smallint range")
	}
	return int16(value), nil
}

func dialogFilterInt32(value int) (int32, error) {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, errors.New("dialog filter value outside integer range")
	}
	return int32(value), nil
}

func rollbackDialogFilterMutation(tx pgx.Tx, cause error) error {
	if err := tx.Rollback(context.Background()); err != nil {
		return errors.Join(cause, fmt.Errorf("rollback dialog filter mutation: %w", err))
	}
	return cause
}
