package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// PeerDialogKey identifies one peer in the namespace accepted by
// messages.getPeerDialogs. PeerType is part of the key because user, chat, and
// channel ids are independent namespaces.
type PeerDialogKey struct {
	PeerType PeerType
	PeerID   int64
}

// PeerDialog is one selected dialog and the top row that belongs to it. A
// channel has a channel message instead of an owner-scoped Message and carries
// its channel pts separately.
type PeerDialog struct {
	Dialog         Dialog
	Pts            int
	Message        *Message
	ChannelMessage *ChannelMessage
}

// PeerDialogsSnapshot is the complete read set needed to render one
// messages.peerDialogs response. It is assembled in one repeatable-read,
// read-only transaction so selection, entitlement, hydration, and state all
// describe the same database snapshot.
type PeerDialogsSnapshot struct {
	Dialogs        []PeerDialog
	State          State
	Users          map[int64]User
	EntitledUsers  map[int64]bool
	Chats          map[int64]Chat
	ChatMembers    map[int64][]Participant
	ChatMembership map[int64]bool
	Channels       map[int64]Channel
	ChannelMembers map[int64]ChannelMember
	Files          map[int64]File
}

// SetPeerDialogsSnapshotHook installs the test-only synchronization seam used
// by API concurrency tests. Production callers leave it nil, so it has no
// effect on the read path outside tests.
func SetPeerDialogsSnapshotHook(s *Store, fn func()) { s.peerDialogsSnapshotHook = fn }

// PeerDialogsSnapshot selects and hydrates the requested peers for ownerID.
// The caller has already validated the input constructors and access hashes;
// this method only reads the exact owner-scoped rows that those keys name.
func (s *Store) PeerDialogsSnapshot(ctx context.Context, ownerID int64, peers []PeerDialogKey) (PeerDialogsSnapshot, error) {
	snapshot := PeerDialogsSnapshot{
		Users:          map[int64]User{},
		EntitledUsers:  map[int64]bool{},
		Chats:          map[int64]Chat{},
		ChatMembers:    map[int64][]Participant{},
		ChatMembership: map[int64]bool{},
		Channels:       map[int64]Channel{},
		ChannelMembers: map[int64]ChannelMember{},
		Files:          map[int64]File{},
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("begin peer dialogs snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	if s.peerDialogsSnapshotHook != nil {
		s.peerDialogsSnapshotHook()
	}
	qtx := s.q.WithTx(tx)

	userIDs, chatIDs, channelIDs := peerIDLists(peers)
	dialogRows, err := qtx.PeerDialogsForOwner(ctx, db.PeerDialogsForOwnerParams{
		OwnerID: ownerID,
		UserIds: userIDs,
		ChatIds: chatIDs,
	})
	if err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("peer dialogs: %w", err)
	}

	selected := make(map[PeerDialogKey]PeerDialog, len(dialogRows)+len(channelIDs))
	localIDs := make([]int64, 0, len(dialogRows))
	for _, row := range dialogRows {
		d := Dialog{
			OwnerID:         row.OwnerID,
			PeerType:        PeerType(row.PeerType),
			PeerID:          row.PeerID,
			TopMessage:      row.TopMessage,
			UnreadCount:     int(row.UnreadCount),
			ReadInboxMaxID:  row.ReadInboxMaxID,
			ReadOutboxMaxID: row.ReadOutboxMaxID,
		}
		selected[PeerDialogKey{PeerType: d.PeerType, PeerID: d.PeerID}] = PeerDialog{Dialog: d}
		localIDs = append(localIDs, d.TopMessage)
	}

	messageRows, err := qtx.MessagesByOwnerLocals(ctx, db.MessagesByOwnerLocalsParams{
		OwnerID:  ownerID,
		LocalIds: localIDs,
	})
	if err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("peer dialog messages: %w", err)
	}
	messages := make(map[int64]Message, len(messageRows))
	for _, row := range messageRows {
		messages[row.LocalID] = messageFromRow(row)
	}
	for key, d := range selected {
		if m, ok := messages[d.Dialog.TopMessage]; ok {
			mCopy := m
			d.Message = &mCopy
			selected[key] = d
		}
	}

	channelRows, err := qtx.PeerChannelDialogsForOwner(ctx, db.PeerChannelDialogsForOwnerParams{
		OwnerID:    ownerID,
		ChannelIds: channelIDs,
	})
	if err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("peer channel dialogs: %w", err)
	}
	for _, row := range channelRows {
		channel := channelFromPeerDialogRow(row)
		member := channelMemberFromPeerDialogRow(row, ownerID)
		post := channelMessageFromPeerDialogRow(row)
		d := Dialog{
			OwnerID:    ownerID,
			PeerType:   PeerTypeChannel,
			PeerID:     channel.ID,
			TopMessage: post.LocalID,
		}
		selected[PeerDialogKey{PeerType: PeerTypeChannel, PeerID: channel.ID}] = PeerDialog{
			Dialog:         d,
			Pts:            int(row.ChannelPts),
			ChannelMessage: &post,
		}
		snapshot.Channels[channel.ID] = channel
		snapshot.ChannelMembers[channel.ID] = member
	}

	snapshot.Dialogs = make([]PeerDialog, 0, len(peers))
	for _, peer := range peers {
		if d, ok := selected[peer]; ok {
			snapshot.Dialogs = append(snapshot.Dialogs, d)
		}
	}

	chatIDs = selectedChatIDs(snapshot.Dialogs)
	chatRows, err := qtx.ChatsByIDs(ctx, chatIDs)
	if err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("peer dialog chats: %w", err)
	}
	for _, row := range chatRows {
		snapshot.Chats[row.ID] = chatFromRow(row)
	}
	participantRows, err := qtx.ChatParticipantsByChatIDs(ctx, chatIDs)
	if err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("peer dialog chat members: %w", err)
	}
	for _, row := range participantRows {
		snapshot.ChatMembers[row.ChatID] = append(snapshot.ChatMembers[row.ChatID], Participant{
			UserID:    row.UserID,
			InviterID: row.InviterID,
			Date:      row.Date.Time,
		})
	}
	for chatID := range snapshot.Chats {
		for _, member := range snapshot.ChatMembers[chatID] {
			if member.UserID == ownerID {
				snapshot.ChatMembership[chatID] = true
				break
			}
		}
	}

	userIDs = peerDialogUserIDs(ownerID, snapshot.Dialogs, snapshot.ChatMembers, snapshot.ChatMembership)
	userRows, err := qtx.UsersByID(ctx, userIDs)
	if err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("peer dialog users: %w", err)
	}
	for _, row := range userRows {
		snapshot.Users[row.ID] = UserFromDB(db.UserByIDRow(row))
	}
	entitledRows, err := qtx.EntitledUserIDs(ctx, db.EntitledUserIDsParams{ViewerID: ownerID, Ids: userIDs})
	if err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("peer dialog user entitlement: %w", err)
	}
	for i, row := range entitledRows {
		id, ok := row.(int64)
		if !ok {
			return PeerDialogsSnapshot{}, fmt.Errorf("peer dialog user entitlement: unexpected type %T at row %d", row, i)
		}
		snapshot.EntitledUsers[id] = true
	}

	fileIDs := peerDialogFileIDs(snapshot.Dialogs)
	fileRows, err := qtx.FilesByIDs(ctx, fileIDs)
	if err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("peer dialog files: %w", err)
	}
	for _, row := range fileRows {
		snapshot.Files[row.ID] = fileFromRow(row)
	}

	stateRow, err := qtx.GetState(ctx, ownerID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Fresh accounts have the zero state until their first update row.
	case err != nil:
		return PeerDialogsSnapshot{}, fmt.Errorf("peer dialog state: %w", err)
	default:
		snapshot.State.Pts = int(stateRow.Pts)
		snapshot.State.Qts = int(stateRow.Qts)
		snapshot.State.Seq = int(stateRow.Seq)
		snapshot.State.Date = int(stateRow.Date.Time.Unix())
	}
	unread, err := qtx.UnreadCountForOwner(ctx, ownerID)
	if err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("peer dialog unread count: %w", err)
	}
	snapshot.State.UnreadCount = int(unread)

	if err := tx.Commit(ctx); err != nil {
		return PeerDialogsSnapshot{}, fmt.Errorf("commit peer dialogs snapshot: %w", err)
	}
	return snapshot, nil
}

func peerIDLists(peers []PeerDialogKey) (users, chats, channels []int64) {
	for _, peer := range peers {
		switch peer.PeerType {
		case PeerTypeUser:
			users = append(users, peer.PeerID)
		case PeerTypeChat:
			chats = append(chats, peer.PeerID)
		case PeerTypeChannel:
			channels = append(channels, peer.PeerID)
		}
	}
	return users, chats, channels
}

func selectedChatIDs(dialogs []PeerDialog) []int64 {
	seen := make(map[int64]bool)
	ids := make([]int64, 0, len(dialogs))
	for _, d := range dialogs {
		if d.Dialog.PeerType != PeerTypeChat || seen[d.Dialog.PeerID] {
			continue
		}
		seen[d.Dialog.PeerID] = true
		ids = append(ids, d.Dialog.PeerID)
	}
	return ids
}

func peerDialogUserIDs(ownerID int64, dialogs []PeerDialog, chatMembers map[int64][]Participant, chatMembership map[int64]bool) []int64 {
	seen := map[int64]bool{ownerID: true}
	ids := []int64{ownerID}
	add := func(id int64) {
		if id != 0 && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	for _, d := range dialogs {
		switch d.Dialog.PeerType {
		case PeerTypeUser:
			add(d.Dialog.PeerID)
		case PeerTypeChat:
			if d.Message == nil {
				continue
			}
			add(d.Message.FromID)
			switch d.Message.Action {
			case ChatActionAddUser, ChatActionDeleteUser:
				add(d.Message.ActionUserID)
			case ChatActionCreate:
				if chatMembership[d.Dialog.PeerID] {
					for _, member := range chatMembers[d.Dialog.PeerID] {
						add(member.UserID)
					}
				}
			}
		case PeerTypeChannel:
			if d.ChannelMessage != nil {
				add(d.ChannelMessage.FromID)
			}
		}
	}
	return ids
}

func peerDialogFileIDs(dialogs []PeerDialog) []int64 {
	seen := map[int64]bool{}
	ids := make([]int64, 0, len(dialogs))
	add := func(id int64) {
		if id != 0 && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	for _, d := range dialogs {
		if d.Message != nil {
			add(d.Message.FileID)
		}
		if d.ChannelMessage != nil && d.ChannelMessage.FileID != nil {
			add(*d.ChannelMessage.FileID)
		}
	}
	return ids
}

func channelFromPeerDialogRow(row db.PeerChannelDialogsForOwnerRow) Channel {
	return Channel{
		ID:              row.ChannelID,
		Title:           row.ChannelTitle,
		About:           row.ChannelAbout,
		CreatorID:       row.ChannelCreatorID,
		Megagroup:       row.ChannelMegagroup,
		Version:         int(row.ChannelVersion),
		Date:            row.ChannelDate.Time,
		PinnedMessageID: row.ChannelPinnedMessageID,
		Username:        row.ChannelUsername,
	}
}

func channelMemberFromPeerDialogRow(row db.PeerChannelDialogsForOwnerRow, ownerID int64) ChannelMember {
	return channelMemberFromRow(db.ChannelParticipant{
		ChannelID:   row.ChannelID,
		UserID:      ownerID,
		Role:        row.MemberRole,
		BannedUntil: row.MemberBannedUntil,
		JoinPts:     row.MemberJoinPts,
	})
}

func channelMessageFromPeerDialogRow(row db.PeerChannelDialogsForOwnerRow) ChannelMessage {
	return channelMessageFromFields(channelMsgFields{
		ChannelID:    row.ChannelID,
		LocalID:      row.TopLocalID,
		FromID:       row.TopFromID,
		Date:         row.TopDate,
		Message:      row.TopMessage,
		EditDate:     row.TopEditDate,
		Deleted:      false,
		RandomID:     row.TopRandomID,
		FileID:       row.TopFileID,
		ReplyToMsgID: row.TopReplyToMsgID,
	})
}
