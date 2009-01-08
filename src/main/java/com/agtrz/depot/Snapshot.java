package com.agtrz.depot;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.goodworkalan.memento.Bag;
import com.goodworkalan.memento.Janitor;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Tree;

public final class Snapshot
{
    private final Tree<Record, Mutator> snapshots;

    private final Map<String, BinCommon> mapOfBinCommons;

    private final Map<String, BinSchema> mapOfBinSchemas;

    private final Map<String, JoinSchema> mapOfJoinSchemas;

    private final Map<Long, Janitor> mapOfJanitors;

    private final Set<Long> setOfCommitted;

    private final Mutator mutator;

    private final Map<String, Bin> mapOfBins;
    
    private final Map<String, Join> mapOfJoins;
    
    private final WeakHashMap<Object, Bag> mapOfBoxes;

    private final Long version;

    private final Test test;

    private final Long oldest;

    private boolean spent;

    private final Sync sync;
    
    private final Schema schema;

    public Snapshot(Tree<Record, Mutator> snapshots,
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
        this.mapOfBins = new HashMap<String, Bin>();
//        this.mapOfSwags = new HashMap<Class<?>, Swag>();
        this.mapOfJoins = new HashMap<String, Join>();
        this.version = version;
        this.test = test;
        this.setOfCommitted = setOfCommitted;
        this.oldest = (Long) setOfCommitted.iterator().next();
        this.mapOfJanitors = new HashMap<Long, Janitor>();
        this.sync = sync;
        this.schema = schema;
        this.mapOfIds = new WeakHashMap<Object, Box>();
    }

    public Join getJoin(String joinName)
    {
        Join join = (Join) mapOfJoins.get(joinName);
        if (join == null)
        {
            Join.Schema schema = (Join.Schema) mapOfJoinSchemas.get(joinName);
            join = new Join(this, mutator, schema, joinName, mapOfJanitors);
            mapOfJoins.put(joinName, join);
        }
        return join;
    }
    
    public Bin getBin(Class<?> klass)
    {
        Bin swag = mapOfBins.get(klass);

        if (swag == null)
        {
            Swag.Schema newSwag = schema.getSwagSchema(klass);
            swag = newSwag.newSwag(this, mutator);
            mapOfSwags.put(klass, swag);
        }
        
        return swag;
    }

    public <T> void add(T object)
    {
        Box id = getSwag(object.getClass()).add(object);
        
        mapOfIds.put(object, id);
    }
    
    public <T> void update(long key, T object)
    {
        Box id = getSwag(object.getClass()).update(key, object);
        mapOfIds.put(object, id);
    }
    
    public void delete(Class<?> klass, long key)
    {
        getSwag(klass).delete(key);
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

    public void dump(ObjectOutputStream out) throws IOException
    {
        out.writeObject(new Restoration.Schema(mapOfBinSchemas, mapOfJoinSchemas));

        for (String name : mapOfBinSchemas.keySet())
        {
            Bin.Cursor bags = getBin(name).first();
            while (bags.hasNext())
            {
                Bag bag = bags.nextBag();
                out.writeObject(new Restoration.Bag(name, bag.getKey(), bag.getObject()));
            }
        }

        for (String name : mapOfJoinSchemas.keySet())
        {
            Join.Cursor links = getJoin(name).find(new HashMap<String, Long>());
            while (links.hasNext())
            {
                Tuple tuple = (Tuple) links.nextTuple();
                out.writeObject(new Restoration.Join(name, tuple.getKeys()));
            }
        }
    }

    public void commit()
    {
        if (spent)
        {
            throw new Danger("commit.spent.snapshot", 501);
        }

        spent = true;
        
        for (Swag swag : mapOfSwags.values())
        {
            swag.flush();
        }

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
            for (Swag swag : mapOfSwags.values())
            {
                swag.commit();
            }

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
                Snapshot.Record record = (Snapshot.Record) object;
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