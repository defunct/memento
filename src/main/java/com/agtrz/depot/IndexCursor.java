package com.agtrz.depot;

import java.util.Iterator;

import com.goodworkalan.memento.Bag;
import com.goodworkalan.strata.Strata;

public class IndexCursor
implements Iterator<Object>
{
    private final Comparable<?>[] fields;

    private final Transaction txn;

    private final Strata.Cursor isolated;

    private final Strata.Cursor stored;

    private final boolean limit;

    private Record nextStored;

    private Record nextIsolated;

    private Bag next;

    public IndexCursor(Strata.Cursor stored, Strata.Cursor isolated, Transaction txn, Comparable<?>[] fields, boolean limit)
    {
        this.txn = txn;
        this.fields = fields;
        this.limit = limit;
        this.isolated = isolated;
        this.stored = stored;
        this.nextStored = next(stored, false);
        this.nextIsolated = next(isolated, true);
        this.next = seekBag();
    }

    private IndexRecord next(Strata.Cursor cursor, boolean isolated)
    {
        while (cursor.hasNext())
        {
            Record record = (Record) cursor.next();
            Bag bag = txn.bin.get(txn.schema.unmarshaller, record.key);
            if (bag == null || !bag.getVersion().equals(record.version))
            {
                continue;
            }
            if (limit && !partial(fields, txn.schema.extractor.getFields(bag.getObject())))
            {
                cursor.release();
                return null;
            }
            return record;
        }
        cursor.release();
        return null;
    }

    private Bag seekBag()
    {
        Bag bag = null;
        if (nextIsolated != null || nextStored != null)
        {
            Record next = null;
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
                int compare = compare(txn.getFields(nextIsolated.key, nextIsolated.version), txn.getFields(nextStored.key, nextStored.version));
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
            bag = txn.getBag(next);
            if (!bag.getVersion().equals(next.version))
            {
                bag = nextBag();
            }
        }
        return bag;
    }

    public Bag nextBag()
    {
        Bag bag = next;
        next = seekBag();
        return bag;
    }

    public boolean hasNext()
    {
        return next != null;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public Object next()
    {
        return nextBag().getObject();
    }

    public void release()
    {
        isolated.release();
        stored.release();
    }
}
