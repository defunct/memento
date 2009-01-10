package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Strata;

public final class Snapshot
{
    private final Strata<SnapshotRecord, Long> snapshots;

    private final Map<String, BinCommon> mapOfBinCommons;

    private final Map<String, BinSchema> mapOfBinSchemas;

    private final Map<String, JoinSchema> mapOfJoinSchemas;

    private final Map<Long, Janitor> mapOfJanitors;

    private final Set<Long> setOfCommitted;

    private final Mutator mutator;

    private final Map<Class<?>, Bin> mapOfBins;
    
    private final Map<String, Join> mapOfJoins;

    private final Long version;

    private final Test test;

    private final Long oldest;

    private boolean spent;

    private final Sync sync;
    
    private final WeakIdentityLookup outstandingKeys;
    
    private final WeakHashMap<Long, Object> outstandingValues;
    
    private final BinTable bins = null;

    public Snapshot(Strata<SnapshotRecord, Long> snapshots,
                    Map<String, BinCommon> mapOfBinCommons,
                    Map<String, BinSchema> mapOfBinSchemas,
                    Map<String, JoinSchema> mapOfJoinSchemas,
                    Mutator mutator,
                    Set<Long> setOfCommitted,
                    Test test,
                    Long version,
                    Sync sync)
    {
        this.snapshots = snapshots;
        this.mapOfBinCommons = mapOfBinCommons;
        this.mapOfBinSchemas = mapOfBinSchemas;
        this.mapOfJoinSchemas = mapOfJoinSchemas;
        this.mutator = mutator;
        this.mapOfBins = new HashMap<Class<?>, Bin>();
//        this.mapOfSwags = new HashMap<Class<?>, Swag>();
        this.mapOfJoins = new HashMap<String, Join>();
        this.version = version;
        this.test = test;
        this.setOfCommitted = setOfCommitted;
        this.oldest = (Long) setOfCommitted.iterator().next();
        this.mapOfJanitors = new HashMap<Long, Janitor>();
        this.sync = sync;
        this.outstandingKeys = new WeakIdentityLookup();
        this.outstandingValues = new WeakHashMap<Long, Object>();
    }
    
    public long id(Object item)
    {
        return outstandingKeys.get(item);
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
            throw new Danger("commit.spent.snapshot", 501);
        }

        spent = true;
        
        for (Bin bin : mapOfBins.values())
        {
            bin.flush();
        }

        for (Join join : mapOfJoins.values())
        {
            join.flush();
        }

        try
        {
            for (Bin bin : mapOfBins.values())
            {
                bin.commit();
            }

            for (Join join : mapOfJoins.values())
            {
                join.commit();
            }
        }
        catch (Error e)
        {
            test.changesWritten();

            for (Map.Entry<Long, Janitor> entry : mapOfJanitors.entrySet())
            {
                entry.getValue().rollback(this);

                mutator.free(entry.getKey());
                entry.getValue().dispose(mutator, true);
            }

            mutator.commit();

            snapshots.query(Fossil.initialize(new Stash(), mutator)).remove(version);

            test.journalComplete.release();

            throw e;
        }

        test.changesWritten();

        for (Map.Entry<Long, Janitor> entry : mapOfJanitors.entrySet())
        {
            mutator.free(entry.getKey());
            entry.getValue().dispose(mutator, false);
        }

        Query<SnapshotRecord, Long> query = snapshots.query(Fossil.initialize(new Stash(), mutator));

        SnapshotRecord committed = new SnapshotRecord(version, Depot.COMMITTED);
        query.add(committed);
        query.remove(version);

        test.journalComplete.release();

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

            Query<SnapshotRecord, Long> query = snapshots.query(Fossil.initialize(new Stash(), mutator));

            query.remove(version);

            sync.release();
        }
    }
}