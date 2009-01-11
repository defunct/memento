package com.goodworkalan.memento;

import java.util.Iterator;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Strata;

public final class BinJanitor<T>
implements Janitor
{
    private static final long serialVersionUID = 20070826L;

    private final Strata<BinRecord, Long> isolation;

    private final Item<T> itemClass;

    public BinJanitor(Query<BinRecord, Long> isolation, Item<T> item)
    {
        this.isolation = isolation.getStrata();
        this.itemClass = item;
    }

    public void rollback(Snapshot snapshot)
    {
        Bin<T> bin = snapshot.bin(itemClass);
        Cursor<BinRecord> cursor = isolation.query(Fossil.initialize(new Stash(), bin.mutator)).first();
        while (cursor.hasNext())
        {
            BinRecord record =  cursor.next();
            Iterator<T> indices = bin.mapOfIndices.values().iterator();
            while (indices.hasNext())
            {
                Index index = (Index) indices.next();
                index.remove(bin.mutator, bin, record.key, record.version);
            }
            bin.query.remove(bin.query.extract(record));
        }
        cursor.release();
        bin.query.flush();
    }

    public void dispose(Mutator mutator, boolean deallocate)
    {
        Query<BinRecord, Long> query = isolation.query(Fossil.initialize(new Stash(), mutator));
        if (deallocate)
        {
            Cursor<BinRecord> cursor = query.first();
            while (cursor.hasNext())
            {
                BinRecord record = cursor.next();
                mutator.free(record.address);
            }
        }
        query.destroy();
    }
}
