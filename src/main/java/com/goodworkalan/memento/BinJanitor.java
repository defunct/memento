package com.goodworkalan.memento;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Strata;

public final class BinJanitor<T>
implements Janitor
{
    private static final long serialVersionUID = 20070826L;

    private final Strata<BinRecord> isolation;

    private final Ilk<T> ilk;
    
    private IndexTable<T> indexes;

    public BinJanitor(Query<BinRecord> isolation, Ilk<T> ilk)
    {
        this.isolation = isolation.getStrata();
        this.ilk = ilk;
    }

    public void rollback(Snapshot snapshot)
    {
        Bin<T> bin = snapshot.bin(ilk);
        Cursor<BinRecord> cursor = isolation.query(Fossil.newStash(bin.mutator)).first();
        while (cursor.hasNext())
        {
            BinRecord record =  cursor.next();
            for (IndexMutator<T, ?> indexMutator : indexes)
            {
                indexMutator.remove(bin.mutator, bin, record.key, record.version);
            }
            bin.query.remove(bin.query.comparable(record));
        }
        cursor.release();
    }

    public void dispose(Mutator mutator, boolean deallocate)
    {
        Query<BinRecord> query = isolation.query(Fossil.initialize(new Stash(), mutator));
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
