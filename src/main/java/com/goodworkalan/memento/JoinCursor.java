package com.goodworkalan.memento;

import java.util.Iterator;
import java.util.Map;

import com.goodworkalan.strata.Cursor;


public class JoinCursor
implements Iterator<Joint>
{
    private final JoinAdvancer stored;

    private final JoinAdvancer isolated;

    private final Snapshot snapshot;

    private final long[] keys;

//    private final JoinSchema schema;

    private JoinRecord nextStored;

    private JoinRecord nextIsolated;

    private JoinRecord next;

    private final Map<String, Long> mapToScan;

//    private final JoinIndex index;

    public JoinCursor(Snapshot snapshot, long[] keys, Map<String, Long> mapToScan, Cursor<JoinRecord> storedCursor, Cursor<JoinRecord> isolatedCursor, JoinSchema schema, JoinIndex index)
    {
        this.snapshot = snapshot;
        this.keys = keys;
        this.stored = new JoinAdvancer(storedCursor);
        this.isolated = new JoinAdvancer(isolatedCursor);
        this.mapToScan = mapToScan;
//        this.index = index;
        this.nextStored = stored.advance() ? next(stored, false) : null;
        this.nextIsolated = isolated.advance() ? next(isolated, true) : null;
        this.next = nextRecord();
//        this.schema = schema;
    }

    private JoinRecord next(JoinAdvancer cursor, boolean isolated)
    {
        JoinRecord candidate = null;
        long[] candidateKeys = null;
        for (;;)
        {
            if (cursor.getAtEnd())
            {
                cursor.release();
                break;
            }
            JoinRecord record = cursor.getRecord();
            if (keys.length > 0 && !Store.partial(keys, record.keys))
            {
                cursor.release();
                break;
            }
            if (mapToScan.size() > 0)
            {
//                for (int i = keys.length; i < index.fields.length; i++)
//                {
//                    Long value = (Long) mapToScan.get(index.fields[i]);
//                    if (value != null && !record.keys[i].equals(value))
//                    {
//                        cursor.advance();
//                        continue;
//                    }
//                }
            }
            if (isolated || snapshot.isVisible(record.version))
            {
                if (candidateKeys == null)
                {
                    candidateKeys = record.keys;
                }
                else if (!Store.partial(candidateKeys, record.keys))
                {
                    break;
                }
                candidate = record;
            }
            cursor.advance();
        }
        return candidate;
    }

    private JoinRecord nextRecord()
    {
        JoinRecord next = null;
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
                int compare = Store.compare(nextIsolated.keys, nextStored.keys);
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

    public Joint next()
    {
        Joint joint = null; // new Joint(snapshot, schema.mapOfFields, index.fields, next);
        next = nextRecord();
        return joint;
    }

    public void release()
    {
        stored.release();
        isolated.release();
    }
}