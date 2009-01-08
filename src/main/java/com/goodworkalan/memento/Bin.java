package com.goodworkalan.memento;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Record;
import com.goodworkalan.strata.Strata;
import com.goodworkalan.strata.Transaction;

// FIXME Vacuum.
public final class Bin
{
    private final Class<?> type;

     final Mutator mutator;

    private final Snapshot snapshot;

    private final BinCommon common;

    private final BinSchema schema;

    final Map<String, Index> mapOfIndices;

    private final Query<BinRecord> query;

    private final Transaction<BinRecord, Mutator> isolation;

    public Bin(Snapshot snapshot, Class<?> type, Mutator mutator, String name, BinCommon common, BinSchema schema, Map<Long, Janitor> mapOfJanitors)
    {
        query = schema.getStrata().query(mutator);
        isolation = new BinTree().create(mutator);
        BinJanitor janitor = new BinJanitor(isolation, type);

        PackOutputStream allocation = new PackOutputStream(mutator);
        try
        {
            ObjectOutputStream out = new ObjectOutputStream(allocation);
            out.writeObject(janitor);
        }
        catch (IOException e)
        {
            throw new Danger("Cannot write output stream.", e, 0);
        }

        long address = allocation.temporary();
        mapOfJanitors.put(address, janitor);

        this.snapshot = snapshot;
        this.type = type;
        this.common = common;
        this.mapOfIndices = newIndexMap(snapshot, schema);
        this.schema = schema;
        this.mutator = mutator;
    }

    private static Map<String, Index> newIndexMap(Snapshot snapshot, BinSchema schema)
    {
        Map<String, Index> mapOfIndices = new HashMap<String, Index>();
        Iterator<Map.Entry<String, IndexSchema>> entries = schema.mapOfIndexSchemas.entrySet().iterator();
        while (entries.hasNext())
        {
            Map.Entry<String, IndexSchema> entry = entries.next();
            IndexSchema indexSchema = entry.getValue();
            mapOfIndices.put(entry.getKey(), new Index(indexSchema));
        }
        return mapOfIndices;
    }

    public Class<?> getType()
    {
        return type;
    }

    public void load(Marshaller marshaller, Iterator<Bag> iterator)
    {
        // FIXME Either check for empty bin or determine if existing
        // commit logic will detect collision.
        while (iterator.hasNext())
        {
            Bag bag = (Bag) iterator.next();

            PackOutputStream allocation = new PackOutputStream(mutator);
            marshaller.marshall(allocation, bag.getObject());

            long address = allocation.allocate();

            BinRecord record = new BinRecord(bag.getKey(), bag.getVersion(), address);

            isolation.add(record);

            for (Map.Entry<String, Index> entry : mapOfIndices.entrySet())
            {
                try
                {
                    entry.getValue().add(snapshot, mutator, this, bag);
                }
                catch (Error e)
                {
                    e.put("index", entry.getKey());
                    isolation.remove(record);
                    mutator.free(record.address);
                    throw e;
                }
            }
        }
    }

    private void restore(Long key, Object object)
    {
        Bag bag = new Bag(key, snapshot.getVersion(), object);
        insert(bag);
        if (common.identifier <= key.intValue())
        {
            common.identifier = key.intValue() + 1;
        }
    }

    private void insert(Bag bag)
    {
        PackOutputStream allocation = new PackOutputStream(mutator);

        schema.schema.marshaller.marshall(allocation, bag.getObject());

        long address = allocation.allocate();
        BinRecord record = new BinRecord(bag.getKey(), bag.getVersion(), address);
        isolation.add(record);

        for (Map.Entry<String, Index> entry : mapOfIndices.entrySet())
        {
            try
            {
                ((Index) entry.getValue()).add(snapshot, mutator, this, bag);
            }
            catch (Error e)
            {
                e.put("index", entry.getKey());
                isolation.remove(record);
                mutator.free(record.address);
                throw e;
            }
        }
    }

    public Bag add(Object object)
    {
        Bag bag = new Bag(common.nextIdentifier(), snapshot.getVersion(), object);

        insert(bag);

        return bag;
    }

    private static boolean isDeleted(BinRecord record)
    {
        return record.address == Pack.NULL_ADDRESS;
    }

    private BinRecord update(Long key)
    {
        BinRecord record = isolation.remove(new Comparable[] { key });
        if (record != null)
        {
            mutator.free(record.address);
        }
        else
        {
            record = get(query.find(new Comparable[] { key }), key, false);
        }
        if (record != null)
        {
            record = isDeleted(record) ? null : record;
        }
        return record;
    }

    public Bag update(Long key, Object object)
    {
        Record record = update(key);

        if (record == null)
        {
            throw new Danger("update.bag.does.not.exist", 401);
        }

        Bag bag = new Bag(key, snapshot.getVersion(), object);
        PackOutputStream allocation = new PackOutputStream(mutator);
        schema.schema.marshaller.marshall(allocation, object);
        long address = allocation.allocate();
        isolation.insert(new Record(bag.getKey(), bag.getVersion(), address));

        Iterator<Index> indices = mapOfIndices.values().iterator();
        while (indices.hasNext())
        {
            Index index = (Index) indices.next();
            index.update(snapshot, mutator, this, bag, record.version);
        }

        return bag;
    }

    public void delete(Long key)
    {
        Record record = update(key);

        if (record == null)
        {
            throw new Danger("Deleted record does not exist.", 402);
        }

        if (record.version != snapshot.getVersion())
        {
            isolation.insert(new Record(key, snapshot.getVersion(), Pack.NULL_ADDRESS));
        }
    }

    public void dispose(Record record)
    {

    }

    private BinRecord getVersion(Cursor<BinRecord> cursor, Long key, Long version)
    {
        BinRecord candidate = null;
        while (candidate == null && cursor.hasNext())
        {
            BinRecord record = cursor.next();
            if (!key.equals(record.key))
            {
                break;
            }
            if (version.equals(record.version))
            {
                candidate = record;
            }
        }
        cursor.release();
        return candidate;
    }

    private BinRecord get(Cursor<BinRecord> cursor, Long key, boolean isolated)
    {
        BinRecord candidate = null;
        for (;;)
        {
            if (!cursor.hasNext())
            {
                break;
            }
            BinRecord record = cursor.next();
            if (!key.equals(record.key))
            {
                break;
            }
            if (isolated || snapshot.isVisible(record.version))
            {
                candidate = record;
            }
        }
        cursor.release();
        return candidate;
    }

    private Bag unmarshall(Unmarshaller unmarshaller, BinRecord record)
    {
        ByteBuffer block = mutator.read(record.address);
        Object object = unmarshaller.unmarshall(new ByteBufferInputStream(block));
        return new Bag(record.key, record.version, object);
    }

    private BinRecord getRecord(Long key)
    {
        BinRecord stored = get(query.find(new Comparable[] { key }), key, false);
        BinRecord isolated = get(isolation.find(new Comparable[] { key }), key, true);
        if (isolated != null)
        {
            return isDeleted(isolated) ? null : isolated;
        }
        else if (stored != null)
        {
            return isDeleted(stored) ? null : stored;
        }
        return null;
    }

    private BinRecord getRecord(Long key, Long version)
    {
        BinRecord stored = getVersion(query.find(new Comparable[] { key }), key, version);
        BinRecord isolated = getVersion(isolation.find(new Comparable[] { key }), key, version);
        if (isolated != null)
        {
            return isDeleted(isolated) ? null : isolated;
        }
        else if (stored != null)
        {
            return isDeleted(stored) ? null : stored;
        }
        return null;
    }

    public Bag get(Long key)
    {
        return get(schema.schema.unmarshaller, key);
    }

    public Bag get(Unmarshaller unmarshaller, Long key)
    {
        BinRecord record = getRecord(key);
        return record == null ? null : unmarshall(unmarshaller, record);
    }

    Bag get(Unmarshaller unmarshaller, Long key, Long version)
    {
        BinRecord record = getRecord(key, version);
        return record == null ? null : unmarshall(unmarshaller, record);
    }

    // FIXME Call this somewhere somehow.
    void copacetic()
    {
        Set<Long> seen = new HashSet<Long>();
        Strata.Cursor isolated = isolation.first();
        while (isolated.hasNext())
        {
            Record record = (Record) isolated.next();
            if (seen.contains(record))
            {
                throw new Danger("Duplicate key in isolation.", 0);
            }
            seen.add(record.key);
        }
    }

    void flush()
    {
        isolation.flush();
    }

    void commit()
    {
        Cursor<BinRecord> isolated = isolation.first();
        while (isolated.hasNext())
        {
            query.add(isolated.next());
        }
        query.flush();

        boolean copacetic = true;
        isolated = isolation.first();
        while (copacetic && isolated.hasNext())
        {
            BinRecord record = isolated.next();
            Cursor<BinRecord> cursor = query.find(new Comparable[] { record.key });
            while (copacetic && cursor.hasNext())
            {
                BinRecord candidate = cursor.next();
                
                assert candidate.key == record.key;

                if (candidate.version == record.version)
                {
                    break;
                }
                
                else if (!snapshot.isVisible(candidate.version))
                {
                    copacetic = false;
                }
            }
            cursor.release();
        }

        if (!copacetic)
        {
            throw new Error("Concurrent modification.", CONCURRENT_MODIFICATION_ERROR);
        }

        Iterator<Index> indices = mapOfIndices.values().iterator();
        while (indices.hasNext())
        {
            Index index = indices.next();
            index.commit(snapshot, mutator, this);
        }
    }

    public IndexCursor find(String indexName, Comparable<?>[] fields, boolean limit)
    {
        Index index = (Index) mapOfIndices.get(indexName);
        if (index == null)
        {
            throw new Danger("no.such.index", 503).add(indexName);
        }

        return index.find(snapshot, mutator, this, fields, limit);
    }

    public IndexCursor find(String string, Comparable<?>[] fields)
    {
        return find(string, fields, true);
    }

    public IndexCursor first(String indexName)
    {
        Index index = (Index) mapOfIndices.get(indexName);
        if (index == null)
        {
            throw new Danger("no.such.index", 503).add(indexName);
        }

        return index.first(snapshot, mutator, this);
    }

    public IndexCursor first()
    {
        return first(schema.schema.unmarshaller);
    }

    public Cursor first(Unmarshaller unmarshaller)
    {
        return new Cursor(snapshot, mutator, isolation.first(), schema.getStrata().query(Fossil.txn(mutator)).first(), unmarshaller);
    }
}
