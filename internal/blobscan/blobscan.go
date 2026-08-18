// Package blobscan classifies what is on the blob store against what the
// database accounts for: blobs no files row explains, paths the layout does not
// explain, and writes still in progress.
//
// It is read-only in every direction. It deletes nothing, unlinks nothing,
// writes no row, opens no transaction and takes no lock, so a send, forward,
// upload or download never waits on it. What it produces is a report an
// operator reads before anything is ever enabled to act on one.
//
// Naming is not deciding, and the two classes it names are reclaimed by
// different mechanisms — an abandoned temporary file is a path the writer gave
// up on, an orphan is bytes the database no longer knows about. A third class,
// the paths this package cannot explain, is reported and nothing more: an
// unexpected entry under the blob root belongs to whoever put it there, and a
// reclaimer that guesses at one is how an unrelated file gets destroyed.
package blobscan

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
)

// idBatch is how many ids one row-existence query carries. It bounds the query,
// not the report: the walk keeps going and asks again, in reads whose footprint
// does not grow with the tree.
const idBatch = 1000

// MaxSample is how many paths of one class a report retains. Counts and byte
// totals are exact whatever this is; the paths are a sample for an operator to
// look at, not a work list. A reclaimer wants the ids under its own bound and
// pages them itself, and a tree that is unexpectedly full of one class must not
// let the pass that exists to keep a deployment safe become the thing that
// exhausts its memory.
const MaxSample = 64

// Candidate is one path a report names, with the bytes behind it. It carries no
// access hash and must not grow one: that value is the unguessable half of a
// download credential, and a report is exactly the kind of record that is
// logged whole.
type Candidate struct {
	// Key is the path relative to the blob store root.
	Key string
	// ID is the file id the key names, and zero for a path that names none.
	ID int64
	// Size is the bytes the path holds.
	Size int64
}

// Class is one category of what a pass found: how much of it there is, how many
// bytes it holds, and a bounded sample of the paths.
type Class struct {
	Count int
	Bytes int64
	Paths []Candidate
}

func (c *Class) add(e Candidate) {
	c.Count++
	c.Bytes += e.Size
	if len(c.Paths) < MaxSample {
		c.Paths = append(c.Paths, e)
	}
}

// Report is what one pass saw. The seven outcomes partition Walked: every path
// examined lands in exactly one of them, so "nothing to reclaim" is
// distinguishable from "held back, and by what". Shard directories and the
// parts directory are the remainder — they are the layout itself and are
// reported as nothing.
type Report struct {
	// Through is the highest file id allocated when the walk began. It is the
	// pass's whole safety argument: it is read before the listing starts, so a
	// blob written afterwards is above it and is excluded from every class,
	// because a table read that predates a blob cannot judge it.
	Through int64
	// Walked is how many paths the pass examined, directories included.
	Walked int
	// Orphans are blobs at or below Through that no files row accounts for.
	// They are what a reclaim would free.
	Orphans Class
	// Temps are writes the blob writer abandoned: its temporary file, past the
	// age cutoff. A fresh one is an upload running right now, which is why the
	// cutoff is not optional and why these are counted apart from Orphans —
	// they are reclaimed by unlinking a path, not by an id having no row.
	Temps Class
	// Parts are in-flight upload part objects under the parts prefix. The
	// layout explains them and this pass never names them as anything else;
	// they are counted so an operator sees how many live upload bytes the tree
	// holds, and their reclaim is the upload-part sweep's, not this pass's.
	Parts Class
	// Unexplained are paths the blob layout does not produce. Their bytes are
	// counted so an operator can see the size of what is there, and they are
	// not reclaimable by anything: they are reported and left alone.
	Unexplained Class
	// Accounted is blobs whose files row exists, stored or not.
	Accounted int
	// AboveSnapshot is blobs whose id is above Through, which this pass cannot
	// judge and the next one will.
	AboveSnapshot int
	// TempsInFlight is temporary files newer than the cutoff: uploads in
	// progress, and the reason the cutoff exists.
	TempsInFlight int
}

// Files is the database half of the classification. [store.Store] satisfies it.
type Files interface {
	// AllocatedFileIDCeiling returns the highest file id ever allocated.
	AllocatedFileIDCeiling(ctx context.Context) (int64, error)
	// ExistingFileIDs returns which of ids have a row, stored or not.
	ExistingFileIDs(ctx context.Context, ids []int64) (map[int64]struct{}, error)
}

// Tree is the blob half: enumeration of what is actually on the store.
// [blob.Local] satisfies it.
//
// It is a local interface rather than a method on blob.Store because
// enumeration is only defined on the local backend today. When the store grows
// prefix enumeration for a remote backend, this is what it lands on, and the
// pass keeps working — a classifier that walked a directory that an object
// store leaves empty would report zero orphans and look healthy.
type Tree interface {
	Walk(ctx context.Context, fn func(blob.Entry) error) error
}

// Scan classifies every path under the blob tree against the files table.
// tempOlderThan is the age cutoff for abandoned writes: only a temporary file
// last written strictly before it can be named.
//
// The id snapshot is read before the listing begins, and that order is the
// whole argument. file ids are a never-reused increasing sequence, so a blob at
// or below the snapshot was allocated before the table was read and the read
// says something about it; a blob above it was not, and no verdict on it is
// available from this pass at all. Reversing the two would classify a blob
// written during the walk as unaccounted for, and the bytes of a live message
// would be what a reclaimer acted on.
//
// Naming is not deciding. Nothing here prevents a row from appearing for an id
// a moment after it was named, and nothing here should: what makes an eventual
// reclaim safe is the interlock on the files row taken by every path that
// writes a reference, not this classification and not the cutoff.
func Scan(ctx context.Context, tree Tree, files Files, tempOlderThan time.Time) (Report, error) {
	through, err := files.AllocatedFileIDCeiling(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("blob scan: %w", err)
	}
	rep := Report{Through: through}

	// Blobs are held until a batch is full rather than asked about one at a
	// time: the walk is the expensive half already, and one query per blob
	// would put the report's cost on the database instead.
	pending := make([]Candidate, 0, idBatch)
	resolve := func() error {
		if len(pending) == 0 {
			return nil
		}
		ids := make([]int64, len(pending))
		for i, c := range pending {
			ids[i] = c.ID
		}
		rows, err := files.ExistingFileIDs(ctx, ids)
		if err != nil {
			return err
		}
		for _, c := range pending {
			if _, ok := rows[c.ID]; ok {
				rep.Accounted++
				continue
			}
			rep.Orphans.add(c)
		}
		pending = pending[:0]
		return nil
	}

	partsDir := strings.TrimSuffix(blob.PartsPrefix, "/")

	err = tree.Walk(ctx, func(e blob.Entry) error {
		rep.Walked++
		switch {
		case e.Dir:
			// A directory the layout produces holds blobs and is not itself
			// one: the shards, and the parts prefix's own directory. The parts
			// tree is the layout's other keyspace, so everything under it is a
			// part object, named as such; whatever is not is somebody's, not
			// ours.
			if blob.IsShard(e.Key) || e.Key == partsDir {
				return nil
			}
			rep.Unexplained.add(Candidate{Key: e.Key})
			return nil
		case !e.Regular:
			// A symlink, socket or device never came from the writer, whatever
			// it is named. Classifying it by its name would be the one case
			// where the name is not evidence of anything.
			rep.Unexplained.add(Candidate{Key: e.Key})
			return nil
		}

		// A class is earned by round-tripping through what the writer
		// produces, never by where a path sits. Checked ahead of every other
		// class: the only writer a key under the parts prefix comes from is
		// NewPartKey, and only what it could have produced is a live upload
		// part. A part key names no file id, so it is never an orphan
		// candidate; the writer's temporary file for one is a write in
		// progress like any other, judged by the age cutoff alone; and
		// whatever the prefix holds that parses to neither is unexplained,
		// with its bytes, reported and left alone.
		if strings.HasPrefix(e.Key, blob.PartsPrefix) {
			key, temp := strings.CutSuffix(e.Key, blob.TempSuffix)
			switch {
			case temp:
				// A temporary file is a write in progress, not a stored part:
				// it is judged by the age cutoff alone, in the temporary
				// class, and only what is past the cutoff is reported.
				if !e.ModTime.Before(tempOlderThan) {
					rep.TempsInFlight++
					return nil
				}
				rep.Temps.add(Candidate{Key: e.Key, Size: e.Size})
				return nil
			case blob.ParsePartKey(key):
				rep.Parts.add(Candidate{Key: e.Key, Size: e.Size})
				return nil
			default:
				rep.Unexplained.add(Candidate{Key: e.Key, Size: e.Size})
				return nil
			}
		}

		key, temp := strings.CutSuffix(e.Key, blob.TempSuffix)
		id, ok := blob.ParseKey(key)
		if !ok {
			rep.Unexplained.add(Candidate{Key: e.Key, Size: e.Size})
			return nil
		}
		c := Candidate{Key: e.Key, ID: id, Size: e.Size}
		// Checked ahead of every class, the temporary one included: an id the
		// snapshot predates is out of scope for this pass by construction.
		if id > through {
			rep.AboveSnapshot++
			return nil
		}
		if temp {
			if !e.ModTime.Before(tempOlderThan) {
				rep.TempsInFlight++
				return nil
			}
			rep.Temps.add(c)
			return nil
		}
		pending = append(pending, c)
		if len(pending) < idBatch {
			return nil
		}
		return resolve()
	})
	if err != nil {
		return Report{}, fmt.Errorf("blob scan: %w", err)
	}
	if err = resolve(); err != nil {
		return Report{}, fmt.Errorf("blob scan: %w", err)
	}
	return rep, nil
}
