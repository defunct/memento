package com.goodworkalan.memento;

import java.util.Iterator;
import java.util.Map;

import com.agtrz.depot.Depot;

public class JoinCursor
implements Iterator<Tuple>
{
    private final Join.Advancer stored;

    private final Join.Advancer isolated;

    private final Snapshot snapshot;

    private final Long[] keys;

    private final Join.Schema schema;

    private Join.Record nextStored;

    private Join.Record nextIsolated;

    private Join.Record next;

    private final Map<String, Long> mapToScan;

    private final Join.Index index;

    public Cursor(Snapshot snapshot, Long[] keys, Map<String, Long> mapToScan, Strata.Cursor storedCursor, Strata.Cursor isolatedCursor, Join.Schema schema, Join.Index index)
    {
        this.snapshot = snapshot;
        this.keys = keys;
        this.stored = new Advancer(storedCursor);
        this.isolated = new Advancer(isolatedCursor);
        this.mapToScan = mapToScan;
        this.index = index;
        this.nextStored = stored.advance() ? next(stored, false) : null;
        this.nextIsolated = isolated.advance() ? next(isolated, true) : null;
        this.next = nextRecord();
        this.schema = schema;
    }

    private Join.Record next(Join.Advancer cursor, boolean isolated)
    {
        Join.Record candidate = null;
        Long[] candidateKeys = null;
        for (;;)
        {
            if (cursor.getAtEnd())
            {
                cursor.release();
                break;
            }
            Join.Record record = cursor.getRecord();
            if (keys.length > 0 && !Depot.partial(keys, record.keys))
            {
                cursor.release();
                break;
            }
            if (mapToScan.size() > 0)
            {
                for (int i = keys.length; i < index.fields.length; i++)
                {
                    Long value = (Long) mapToScan.get(index.fields[i]);
                    if (value != null && !record.keys[i].equals(value))
                    {
                        cursor.advance();
                        continue;
                    }
                }
            }
            if (isolated || snapshot.isVisible(record.version))
            {
                if (candidateKeys == null)
                {
                    candidateKeys = record.keys;
                }
                else if (!Depot.partial(candidateKeys, record.keys))
                {
                    break;
                }
                candidate = record;
            }
            cursor.advance();
        }
        return candidate;
    }

    private Join.Record nextRecord()
    {
        Join.Record next = null;
        if (nextIsolated != null || nextStored != null)
        {
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
                int compare = Depot.compare(nextIsolated.keys, nextStored.keys);
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
            if (next.deleted)
            {
                next = nextRecord();
            }
        }
        return next;
    }

    public boolean hasNext()
    {
        return next != null;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public Tuple nextTuple()
    {
        Tuple tuple = new Tuple(snapshot, schema.mapOfFields, index.fields, next);
        next = nextRecord();
        return tuple;
    }

    public Tuple next()
    {
        Tuple tuple = new Tuple(snapshot, schema.mapOfFields, index.fields, next);
        next = nextRecord();
        return tuple;
    }

    public void release()
    {
        stored.release();
        isolated.release();
    }
}