package com.goodworkalan.memento;

import java.util.Set;
import java.util.TreeSet;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;


public class Store
{
    private final static Integer OPERATING = new Integer(1);

    final static Integer COMMITTED = new Integer(2);

    private final BinSchemaTable binSchemas = new BinSchemaTable();

    private final JoinSchemaTable joinSchemas = new JoinSchemaTable();
    
    private final Storage storage;
    
    public Store(Storage storage)
    {
        this.storage = storage;
    }
    
    public <T> BinBuilder<T> store(Class<T> meta)
    {
        return new BinBuilder<T>(this, binSchemas, new Ilk<T>(meta));
    }
    
    public <T> BinBuilder<T> store(Ilk<T> ilk)
    {
        return new BinBuilder<T>(this, binSchemas, ilk);
    }
    
    public LinkBuilder link(Link link)
    {
        return new LinkBuilder(joinSchemas, this, link);
    }

    public Snapshot newSnapshot(Sync sync)
    {
        Long version = new Long(System.currentTimeMillis());
        SnapshotRecord newSnapshot = new SnapshotRecord(version, OPERATING);

        Query<SnapshotRecord> query = storage.newSnapshotQuery();
        Cursor<SnapshotRecord> versions = query.first();
        Set<Long> setOfCommitted = new TreeSet<Long>();
        while (versions.hasNext())
        {
            SnapshotRecord snapshot = versions.next();
            if (snapshot.state == COMMITTED)
            {
                setOfCommitted.add(snapshot.version);
            }
        }
        versions.release();

        query.add(newSnapshot);

        query.getStash().get(Storage.MUTATOR, Mutator.class).commit();

        return new Snapshot(storage, null, setOfCommitted, version, sync);
     }
    
    public Snapshot newSnapshot()
    {
        return null;
    }

    // FIXME Keeping this around, need it, but doesn't belong here.
    public static boolean partial(long[] partial, long[] full)
    {
        for (int i = 0 ; i < partial.length; i++)
        {
            if (partial[i] != full[i])
            {
                return false;
            }
        }
        return true;
    }
    
    // FIXME Keeping this around, need it, but doesn't belong here.
    public static int compare(long[] partial, long[] full)
    {
        int compare = 0;
        for (int i = 0; compare == 0 && i < partial.length; i++)
        {
            compare = partial[i] < full[i] ? -1 : partial[i] > full[i] ? 1 : 0;
        }
        return compare == 0 ? partial.length - full.length : compare;
    }
    
    public void end()
    {
    }
}
