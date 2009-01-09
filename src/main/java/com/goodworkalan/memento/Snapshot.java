package com.goodworkalan.memento;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.goodworkalan.memento.Restoration.Join;
import com.goodworkalan.memento.Restoration.Schema;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Strata;

public final class Snapshot
{
    private final Strata<SnapshotRecord, Mutator> snapshots;

    private final Map<String, BinCommon> mapOfBinCommons;

    private final Map<String, BinSchema> mapOfBinSchemas;

    private final Map<String, JoinSchema> mapOfJoinSchemas;

    private final Map<Long, Janitor> mapOfJanitors;

    private final Set<Long> setOfCommitted;

    private final Mutator mutator;

    private final Map<Class<?>, Bin> mapOfBins;
    
    private final Map<String, Join> mapOfJoins;
    
    private final WeakHashMap<Object, Bag> mapOfBoxes;

    private final Long version;

    private final Test test;

    private final Long oldest;

    private boolean spent;

    private final Sync sync;
    
    private final Schema schema;
    
    private final WeakIdentityLookup outstandingKeys;
    
    private final WeakHashMap<Long, Object> outstandingValues; 

    public Snapshot(Strata<SnapshotRecord, Mutator> snapshots,
                    Schema schema,
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
        this.schema = schema;
        this.outstandingKeys = new WeakIdentityLookup();
        this.outstandingValues = new WeakHashMap<Long, Object>();
    }
    
    public <Item> Class<?> classFor(Item item) 
    {
        return item.getClass();
    }
    
    public long id(Object item)
    {
        return outstandingKeys.get(item);
    }
    
    @SuppressWarnings("unchecked")
    private <Item> Bin<Item> toBin(Class<Item> itemClass, Bin<?> bin)
    {
        return (Bin<Item>) bin;
    }
    
    public <Item> Bin<Item> bin(Class<Item> itemClass)
    {
        // FIXME Populate a snapshot with all bins, no lazy construct.
        Bin<?> bin = mapOfBins.get(itemClass);
        if (bin == null)
        {
            throw new IllegalStateException();
        }
        return toBin(itemClass, bin);
    }
    
    public <Item> Item get(Class<Item> itemClass, long key)
    {
        return bin(itemClass).get(key);
    }

    public Join getJoin(String joinName)
    {
        Join join = (Join) mapOfJoins.get(joinName);
        if (join == null)
        {
            JoinSchema schema = (JoinSchema) mapOfJoinSchemas.get(joinName);
            join = new Join(this, mutator, schema, joinName, mapOfJanitors);
            mapOfJoins.put(joinName, join);
        }
        return join;
    }

    public <Item> void add(Class<Item> itemClass, Item item)
    {
        bin(itemClass).add(item);
    }
    
    public <Item> void update(Class<Item> itemClass, Item item)
    {
        bin(itemClass).update(item);
    }

    public <Item> void delete(Class<Item> itemClass, Item item)
    {
        bin(itemClass).delete(item);
    }

    public <Item> void delete(Class<Item> itemClass, long key)
    {
        bin(itemClass).delete(key);
    }
    
    @SuppressWarnings("unchecked")
    public <T> T load(Class<T> klass, long key)
    {
        assert key > 0;
        
        Swag swag = getSwag(klass);
        
        Box id = swag.get(key);
        
        if (id == null)
        {
            return null;
        }
        
        mapOfIds.put(id.getObject(), id);
        
        return (T) id.getObject();
    }
    
    public long getId(Object object)
    {
        Box id = mapOfIds.get(object);
        if (id == null)
        {
            return 0L;
        }
        return id.getKey();
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

            Strata.Query query = snapshots.query(Fossil.txn(mutator));
            query.remove(new Comparable[] { version }, Strata.ANY);

            test.journalComplete.release();

            throw e;
        }

        test.changesWritten();

        for (Map.Entry<Long, Janitor> entry : mapOfJanitors.entrySet())
        {
            mutator.free(entry.getKey());
            entry.getValue().dispose(mutator, false);
        }

        Strata.Query query = snapshots.query(Fossil.txn(mutator));

        Snapshot.Record committed = new Record(version, Depot.COMMITTED);
        query.insert(committed);

        query.remove(new Comparable[] { version }, Strata.ANY);

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

            Strata.Query query = snapshots.query(Fossil.txn(mutator));

            query.remove(new Comparable[] { version }, Strata.ANY);

            sync.release();
        }
    }


    final static class Writer
    implements Fossil.Writer, Serializable
    {
        private static final long serialVersionUID = 20070409L;

        public void write(ByteBuffer bytes, Object object)
        {
            if (object == null)
            {
                bytes.putLong(0L);
                bytes.putInt(0);
            }
            else
            {
                Snapshot.Record record = (com.goodworkalan.memento.Record) object;
                bytes.putLong(record.version.longValue());
                bytes.putInt(record.state.intValue());
            }
        }
    }

    final static class Reader
    implements Fossil.Reader, Serializable
    {
        private static final long serialVersionUID = 20070409L;

        public Object read(ByteBuffer bytes)
        {
            Long version = new Long(bytes.getLong());
            Integer state = new Integer(bytes.getInt());
            if (version.longValue() == 0L)
            {
                return null;
            }
            Snapshot.Record record = new Record(version, state);
            return record;
        }
    }
}