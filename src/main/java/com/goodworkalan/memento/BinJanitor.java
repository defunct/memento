package com.goodworkalan.memento;

import java.lang.reflect.TypeVariable;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Strata;

// TODO Document.
public final class BinJanitor<T>
implements Janitor
{
    // TODO Document.
    private static final long serialVersionUID = 20070826L;

    // TODO Document.
    private final Strata<BinRecord> isolation;

    /** The super type token of the objects stored in the bin. */
    private final Ilk<T> ilk;
    
    // TODO Document.
    private IndexTable<T> indexes;

    // TODO Document.
    public BinJanitor(Query<BinRecord> isolation, Ilk<T> ilk)
    {
        this.isolation = isolation.getStrata();
        this.ilk = ilk;
    }

    // TODO Document.
    public void rollback(Snapshot snapshot)
    {
        Bin<T> bin = snapshot.bin(ilk);
        Cursor<BinRecord> cursor = isolation.query(Fossil.newStash(bin.mutator)).first();
        while (cursor.hasNext())
        {
            BinRecord record =  cursor.next();
            for (Ilk.Box box : indexes)
            {
                IndexMutator<T, ?> index = box.cast(new Ilk<IndexMutator<T, ?>>() { }.assign((TypeVariable<?>) new Ilk<T>() {}.key.type, ilk.key.type));
                index.remove(bin.mutator, bin, record.key, record.version);
            }
            bin.query.remove(bin.query.comparable(record));
        }
        cursor.release();
    }

    // TODO Document.
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
