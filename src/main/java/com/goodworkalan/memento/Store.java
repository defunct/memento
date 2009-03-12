package com.goodworkalan.memento;

import java.util.Set;
import java.util.TreeSet;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;

// TODO Document.
public class Store
{
    /** A snapshot record state for snapshots in progress. */
    final static int OPERATING = 1;

    /**  A snapshot record state for committed snapshots. */
    final static int COMMITTED = 2;
    
    private final BinSchemaTable binSchemas = new BinSchemaTable();

    private final JoinSchemaTable joinSchemas = new JoinSchemaTable();
    
    /** The storage strategy. */
    private final PackFactory storage;
    
    /**
     * Create a store with the given storage strategy.
     * 
     * @param storage The storage strategy.
     */
    public Store(PackFactory storage)
    {
        this.storage = storage;
    }
    
    // TODO Document.
    public void create()
    {
        storage.create();
    }

    /**
     * Create a bin builder that will store objects of the given class.
     * 
     * @param <T>
     *            The type of object to store.
     * @param meta
     *            The class of the objects to store in the bin.
     * @return Return a bin builder.
     */
    public <T> BinBuilder<T> store(Class<T> meta)
    {
        return new BinBuilder<T>(this, binSchemas, new Ilk<T>(meta));
    }

    /**
     * Create a bin builder that will store objects of the type of the given
     * super type token.
     * 
     * @param <T>
     *            The type of object to store.
     * @param ilk
     *            The super type token of the objects to store in the bin.
     * @return Return a bin builder.
     */
    public <T> BinBuilder<T> store(Ilk<T> ilk)
    {
        return new BinBuilder<T>(this, binSchemas, ilk);
    }
    
    // TODO Document.
    public LinkBuilder link(Link link)
    {
        return new LinkBuilder(joinSchemas, this, link);
    }

    // TODO Document.
    public Snapshot newSnapshot()
    {
        Long version = new Long(System.currentTimeMillis());
        SnapshotRecord newSnapshot = new SnapshotRecord(version, OPERATING);

        // FIXME Broken.
        Query<SnapshotRecord> query = storage.getSnapshots().getStrata().query();
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

//        query.getStash().get(Storage.MUTATOR, Mutator.class).commit();

        return new Snapshot(storage, null, setOfCommitted, version);
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
    
    // TODO Document.
    public void end()
    {
    }
}
