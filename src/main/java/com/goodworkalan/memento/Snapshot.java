package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Strata;

public final class Snapshot
{
    private final Strata<SnapshotRecord> snapshots;
    
//    private final Storage storage;

    private final Map<Long, Janitor> mapOfJanitors;

    private final Set<Long> setOfCommitted;

    private final Mutator mutator;

    private final Long version;

    private final Long oldest;

    private boolean spent;

    private final Sync sync;
    
    private final BinTable bins = null;
    
    private final JoinTable joins;

    public Snapshot(Storage storage,
                    Mutator mutator,
                    Set<Long> setOfCommitted,
                    Long version,
                    Sync sync)
    {
//        this.storage = storage;
        this.snapshots = storage.getSnapshots();
        this.mutator = mutator;
        this.version = version;
        this.setOfCommitted = setOfCommitted;
        this.oldest = (Long) setOfCommitted.iterator().next();
        this.mapOfJanitors = new HashMap<Long, Janitor>();
        this.sync = sync;
        this.joins = new JoinTable(storage, this, mutator, null);
    }
    
    public <T> Bin<T> bin(Item<T> item)
    {
        return bins.get(item);
    }

    public <T> Bin<T> bin(Class<T> itemClass)
    {
        return bins.get(new Item<T>(itemClass) {});
    }
    
    public JoinBuilder join(Link link)
    {
        return null;
    }

    public Long getVersion()
    {
        return version;
    }

    public boolean isVisible(Long version)
    {
        if (oldest.compareTo(version) >= 0)
        {
            return true;
        }
        if (setOfCommitted.contains(version))
        {
            return true;
        }
        return false;
    }

    public void commit()
    {
        if (spent)
        {
            throw new MementoException(115);
        }

        spent = true;
        
        for (Bin<?> bin : bins)
        {
            bin.flush();
        }

        for (Join join : joins)
        {
            join.flush();
        }

        try
        {
            for (Bin<?> bin : bins)
            {
                bin.commit();
            }

            for (Join join : joins)
            {
                join.commit();
            }
        }
        catch (Error e)
        {
            for (Map.Entry<Long, Janitor> entry : mapOfJanitors.entrySet())
            {
                entry.getValue().rollback(this);

                mutator.free(entry.getKey());
                entry.getValue().dispose(mutator, true);
            }

            mutator.commit();

            Query<SnapshotRecord> query = snapshots.query(Fossil.initialize(new Stash(), mutator));
            
            query.remove(new SnapshotVersionComparable(version));

            throw e;
        }

        for (Map.Entry<Long, Janitor> entry : mapOfJanitors.entrySet())
        {
            mutator.free(entry.getKey());
            entry.getValue().dispose(mutator, false);
        }

        Query<SnapshotRecord> query = snapshots.query(Fossil.initialize(new Stash(), mutator));

        SnapshotRecord committed = new SnapshotRecord(version, Store.COMMITTED);
        query.add(committed);
        query.remove(new SnapshotVersionComparable(version));

        sync.release();
    }

    public void rollback()
    {
        // FIXME Rethink. Cannot reuse.
        if (!spent)
        {
            spent = true;
            mutator.commit();
            for (Map.Entry<Long, Janitor> entry : mapOfJanitors.entrySet())
            {
                mutator.free(entry.getKey());
                entry.getValue().dispose(mutator, true);
            }

            Query<SnapshotRecord> query = snapshots.query(Fossil.initialize(new Stash(), mutator));

            query.remove(new SnapshotVersionComparable(version));

            sync.release();
        }
    }
}