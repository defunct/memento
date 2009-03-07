package com.goodworkalan.memento;

import java.nio.ByteBuffer;
import java.util.Iterator;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.pack.io.ByteBufferInputStream;
import com.goodworkalan.strata.Cursor;

public class BinCursor<T>
implements Iterator<Box<T>>
{
    private final Snapshot snapshot;

    private final Mutator mutator;

    private final Cursor<BinRecord> isolation;

    private final Cursor<BinRecord> common;

    private final ItemIO<T> io;

    private Box<T> nextIsolated;

    private Box<T> nextCommon;

    private BinRecord[] firstIsolated;

    private BinRecord[] firstCommon;

    public BinCursor(Snapshot snapshot, Mutator mutator, Cursor<BinRecord> isolation, Cursor<BinRecord> common, ItemIO<T> io)
    {
        this.snapshot = snapshot;
        this.mutator = mutator;
        this.isolation = isolation;
        this.common = common;
        this.io = io;
        this.firstIsolated = new BinRecord[1];
        this.firstCommon = new BinRecord[1];
        this.nextIsolated = next(isolation, firstIsolated, true);
        this.nextCommon = next(common, firstCommon, false);
    }

    public boolean hasNext()
    {
        return !(nextIsolated == null && nextCommon == null);
    }

    private Box<T> next(Cursor<BinRecord> cursor, BinRecord[] first, boolean isolated)
    {
        while (first[0] == null && cursor.hasNext())
        {
            BinRecord record = cursor.next();
            if (isolated || snapshot.isVisible(record.version))
            {
                first[0] = record;
            }
        }
        BinRecord candidate;
        do
        {
            candidate = first[0];
            for (;;)
            {
                if (candidate == null)
                {
                    break;
                }
                if (!cursor.hasNext())
                {
                    cursor.release();
                    first[0] = null;
                    break;
                }
                BinRecord record = cursor.next();
                if (first[0].key != record.key)
                {
                    first[0] = record;
                    break;
                }
                if (isolated || snapshot.isVisible(record.version))
                {
                    candidate = record;
                }
            }
            if (candidate == null)
            {
                return null;
            }
        }
        while (candidate.address == Pack.NULL_ADDRESS);
        ByteBuffer block = mutator.read(candidate.address);
        return new Box<T>(candidate.key, candidate.version, io.read(new ByteBufferInputStream(block)));
    }

    public Box<T> nextBag()
    {
        Box<T> next = null;
        if (nextIsolated == null)
        {
            if (nextCommon != null)
            {
                next = nextCommon;
                nextCommon = next(common, firstCommon, false);
            }
        }
        else if (nextCommon == null)
        {
            next = nextIsolated;
            nextIsolated = next(isolation, firstIsolated, true);
        }
        else
        {
            int compare = new Long(nextIsolated.getKey()).compareTo(nextCommon.getKey());
            if (compare == 0)
            {
                next = nextIsolated;
                nextCommon = next(common, firstCommon, false);
                nextIsolated = next(isolation, firstIsolated, true);
            }
            else if (compare < 0)
            {
                next = nextIsolated;
                nextIsolated = next(isolation, firstIsolated, true);
            }
            else
            {
                next = nextCommon;
                nextCommon = next(common, firstCommon, false);
            }
        }
        return next;
    }

    public Box<T> next()
    {
        return nextBag();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void release()
    {
        isolation.release();
        common.release();
    }
}
