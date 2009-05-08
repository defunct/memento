package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Query;

// TODO Document.
public final class Snapshot
{
    // TODO Document.
    private final PackStrata<SnapshotRecord> snapshots;
    
//    private final Storage storage;

    // TODO Document.
    private final Map<Long, Janitor> mapOfJanitors;

    // TODO Document.
    private final Set<Long> setOfCommitted;

    // TODO Document.
    private final Mutator mutator;

    // TODO Document.
    private final Long version;

    // TODO Document.
    private final Long oldest;

    // TODO Document.
    private boolean spent;
    
    // TODO Document.
    private final BinTable bins = null;
    
    // TODO Document.
    private final JoinTable joins;

    // TODO Document.
    public Snapshot(PackFactory storage, Mutator mutator, Set<Long> setOfCommitted, Long version)
    {
//        this.storage = storage;
        this.snapshots = storage.getSnapshots();
        this.mutator = mutator;
        this.version = version;
        this.setOfCommitted = setOfCommitted;
        this.oldest = (Long) setOfCommitted.iterator().next();
        this.mapOfJanitors = new HashMap<Long, Janitor>();
        this.joins = new JoinTable(storage, this, mutator, null);
    }
    
    // TODO Document.
    public <T> Bin<T> bin(Ilk<T> ilk)
    {
        return bins.get(ilk);
    }

    // TODO Document.
    public <T> Bin<T> bin(Class<T> itemClass)
    {
        return bins.get(new Ilk<T>(itemClass) {});
    }
    
    // TODO Document.
    public JoinBuilder join(Link link)
    {
        return null;
    }

    // TODO Document.
    public Long getVersion()
    {
        return version;
    }

    // TODO Document.
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

    // TODO Document.
    public void commit()
    {
        if (spent)
        {
            throw new MementoException(115);
        }

        spent = true;
        
        for (Ilk.Box box : bins)
        {
            ((Bin <?>) box.getObject()).flush();
        }

        for (Join join : joins)
        {
            join.flush();
        }

        try
        {
            for (Ilk.Box box : bins)
            {
                ((Bin <?>) box.getObject()).commit();
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

            Query<SnapshotRecord> query = snapshots.getStrata().query(Fossil.initialize(new Stash(), mutator));
            
            query.remove(new SnapshotVersionComparable(version));

            throw e;
        }

        for (Map.Entry<Long, Janitor> entry : mapOfJanitors.entrySet())
        {
            mutator.free(entry.getKey());
            entry.getValue().dispose(mutator, false);
        }

        Query<SnapshotRecord> query = snapshots.getStrata().query(Fossil.newStash(mutator));

        SnapshotRecord committed = new SnapshotRecord(version, Store.COMMITTED);
        query.add(committed);
        query.remove(new SnapshotVersionComparable(version));
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

            Query<SnapshotRecord> query = snapshots.getStrata().query(Fossil.initialize(new Stash(), mutator));

            query.remove(new SnapshotVersionComparable(version));
        }
    }
}