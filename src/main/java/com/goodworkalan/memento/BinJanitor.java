package com.goodworkalan.memento;

import java.util.Iterator;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Strata;
import com.goodworkalan.strata.Transaction;

public final class BinJanitor<Item>
implements Janitor
{
    private static final long serialVersionUID = 20070826L;

    private final Strata<BinRecord, Mutator> isolation;

    private final Class<Item> itemClass;

    public BinJanitor(Transaction<BinRecord, Mutator> isolation, Class<Item> itemClass)
    {
        this.isolation = isolation.getStrata();
        this.itemClass = itemClass;
    }

    public void rollback(Snapshot snapshot)
    {
        Bin<Item> bin = snapshot.bin(itemClass);
        Cursor<BinRecord> cursor = isolation.query(bin.mutator).first();
        while (cursor.hasNext())
        {
            BinRecord record =  cursor.next();
            Iterator<Index> indices = bin.mapOfIndices.values().iterator();
            while (indices.hasNext())
            {
                Index index = (Index) indices.next();
                index.remove(bin.mutator, bin, record.key, record.version);
            }
            bin.query.remove(record);
        }
        cursor.release();
        bin.query.flush();
    }

    public void dispose(Mutator mutator, boolean deallocate)
    {
        Query<BinRecord> query = isolation.query(mutator);
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
