package com.goodworkalan.memento;
import static com.goodworkalan.memento.IndexSchema.EXTRACTOR;

import java.util.Iterator;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Cursor;

public class IndexCursor<T, F extends Comparable<F>>
implements Iterator<T>
{
    private final IndexSchema<T, F> indexSchema;

    private final Stash stash;

    private final Cursor<IndexRecord> isolated;

    private final Cursor<IndexRecord> stored;

    private final boolean limit;

    private IndexRecord nextStored;

    private IndexRecord nextIsolated;
    
    private Bin<T> bin;

    private Box<T> next;

    public IndexCursor(IndexSchema<T, F> indexSchema, Cursor<IndexRecord> stored, Cursor<IndexRecord> isolated, Stash stash, boolean limit)
    {
        this.indexSchema = indexSchema;
        this.stash = stash;
        this.limit = limit;
        this.isolated = isolated;
        this.stored = stored;
        this.nextStored = next(stored, false);
        this.nextIsolated = next(isolated, true);
        this.bin = stash.get(EXTRACTOR, BinTable.class).get(indexSchema.getItem());
        this.next = seekBox();
    }

    private IndexRecord next(Cursor<IndexRecord> cursor, boolean isolated)
    {
        while (cursor.hasNext())
        {
            IndexRecord record = cursor.next();
            Box<T> box = stash.get(EXTRACTOR, BinTable.class).get(indexSchema.getItem()).box(record.key);
            if (box == null || box.getVersion() != record.version)
            {
                continue;
            }
            // What did partial mean? Not exactly equal. Test here is for partial.
//            if (limit && !partial(fields, indexSchema.getIndex().getFields(bag.getObject())))
            if (limit)
            {
                cursor.release();
                return null;
            }
            return record;
        }
        cursor.release();
        return null;
    }
    
    private F index(long key, long version)
    {
        return indexSchema.getIndexer().index(bin.box(key, version).getItem());
    }

    private Box<T> seekBox()
    {
        Box<T> box = null;
        if (nextIsolated != null || nextStored != null)
        {
            IndexRecord next = null;
            if (nextIsolated == null)
            {
                next = nextStored;
                nextStored = next(stored, false);
            }
            else if (nextStored == null)
            {
                next = nextIsolated;
                nextIsolated = next(isolated, true);
            }
            else
            {
                int compare = index(nextIsolated.key, nextIsolated.version).compareTo(index(nextStored.key, nextStored.version));
                if (compare < 0)
                {
                    next = nextIsolated;
                    nextIsolated = next(isolated, true);
                }
                else if (compare > 0)
                {
                    next = nextStored;
                    nextStored = next(stored, false);
                }
                else
                {
                    next = nextIsolated;
                    nextIsolated = next(isolated, true);
                    nextStored = next(stored, true);
                }
            }
            box = bin.box(next.key);
            if (box.getVersion() != next.version)
            {
                box = nextBox();
            }
        }
        return box;
    }

    public Box<T> nextBox()
    {
        Box<T> box = next;
        next = seekBox();
        return box;
    }

    public boolean hasNext()
    {
        return next != null;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public T next()
    {
        return nextBox().getItem();
    }

    public void release()
    {
        isolated.release();
        stored.release();
    }
}
