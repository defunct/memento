package com.goodworkalan.memento;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Transaction;

// FIXME Vacuum.
public final class Bin<Item>
{
    final Mutator mutator;

    private final Class<Item> itemClass;
    
    private final Snapshot snapshot;

    private final BinCommon common;

    private final BinSchema<Item> schema;

    final Map<String, Index> mapOfIndices;

    final Query<BinRecord, Long> query;

    private final Transaction<BinRecord, Long, Mutator> isolation;
    
    private final WeakIdentityLookup outstandingKeys;
    
    private final WeakHashMap<Long, Box<Item>> outstandingValues; 

    public Bin(Snapshot snapshot, Class<Item> itemClass, Mutator mutator,
               String name, BinCommon common, BinSchema<Item> schema,
               Map<Long, Janitor> mapOfJanitors)
    {
        query = schema.getStrata().query(mutator);
        isolation = new BinTree().create(mutator);
        BinJanitor<Item> janitor = new BinJanitor<Item>(isolation, itemClass);

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
        this.common = common;
        this.mapOfIndices = newIndexMap(snapshot, schema);
        this.schema = schema;
        this.mutator = mutator;
        
        this.itemClass = itemClass;

        this.outstandingKeys = new WeakIdentityLookup();
        this.outstandingValues = new WeakHashMap<Long, Box<Item>>();
    }

    private static <Item> Map<String, Index> newIndexMap(Snapshot snapshot, BinSchema<Item> schema)
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

    public Class<Item> getItemClass()
    {
        return itemClass;
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
                    isolation.remove(isolation.extract(record));
                    mutator.free(record.address);
                    throw e;
                }
            }
        }
    }

    public void restore(long key, Item object)
    {
        Box<Item> box = new Box<Item>(key, snapshot.getVersion(), object);
        insert(box);
        if (common.identifier <= key)
        {
            common.identifier = key + 1;
        }
    }

    private void insert(Box<Item> box)
    {
        PackOutputStream allocation = new PackOutputStream(mutator);

        schema.io.write(allocation, box.getItem());
 
        long address = allocation.allocate();
        BinRecord record = new BinRecord(box.getKey(), box.getVersion(), address);
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
                isolation.remove(isolation.extract(record));
                mutator.free(record.address);
                throw e;
            }
        }
    }

    public long add(Item item)
    {
        Box<Item> box = new Box<Item>(common.nextIdentifier(), snapshot.getVersion(), item);
        
        insert(box);

        return box.getKey();
    }

    private static boolean isDeleted(BinRecord record)
    {
        return record.address == Pack.NULL_ADDRESS;
    }

    private BinRecord record(Long key)
    {
        BinRecord record = isolation.remove(key);
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
    
    public long update(Item item)
    {
        Long key = outstandingKeys.get(item);

        if (key == null)
        {
            throw new Danger("update.bag.does.not.exist", 401);
        }
        
        return update(key, item).getKey();
    }
    
    public long replace(Item then, Item now)
    {
        Long key = outstandingKeys.get(then);
        
        if (key == null)
        {
            throw new Danger("error", 401);
        }
        
        return update(key, now).getKey();
    }

    public Box<Item> update(long key, Item item)
    {
        BinRecord record = record(key);

        if (record == null)
        {
            throw new Danger("update.bag.does.not.exist", 401);
        }

        PackOutputStream allocation = new PackOutputStream(mutator);
        schema.io.write(allocation, item);
        long address = allocation.allocate();
        
        Box<Item> box = new Box<Item>(key, snapshot.getVersion(), item);
        isolation.add(new BinRecord(box.getKey(), box.getVersion(), address));

        Iterator<Index> indices = mapOfIndices.values().iterator();
        while (indices.hasNext())
        {
            Index index = (Index) indices.next();
            index.update(snapshot, mutator, this, bag, record.version);
        }

        return box;
    }

    public void delete(Item item)
    {
        long key = key(item);
        if (key == 0L)
        {
            throw new IllegalArgumentException();
        }
        delete(key);
    }

    public void delete(long key)
    {
        BinRecord record = record(key);

        if (record == null)
        {
            throw new Danger("Deleted record does not exist.", 402);
        }

        if (record.version != snapshot.getVersion())
        {
            isolation.add(new BinRecord(key, snapshot.getVersion(), Pack.NULL_ADDRESS));
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

    private Box<Item> unmarshall(ItemIO<Item> io, BinRecord record)
    {
        ByteBuffer block = mutator.read(record.address);
        Item item = io.read(new ByteBufferInputStream(block));
        return new Box<Item>(record.key, record.version, item);
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

    public Item get(long key)
    { 
        Box<Item> box = box(key);
        if (box == null)
        {
            return null;
        }
        return box.getItem();
    }

    public long key(Item item)
    {
        Long key = outstandingKeys.get(item);
        if (key == null)
        {
            throw new UnsupportedOperationException();
        }
        return key;
    }

    public Box<Item> box(long key)
    {
        Box<Item> box = outstandingValues.get(key);
        if (box == null)
        {
            /* FIXME */ ItemIO<Item> io = null;
            box = get(io, key);
            outstandingValues.put(box.getKey(), box);
            outstandingKeys.put(box.getItem(), box.getKey());
        }
        return box;
    }

    public Box<Item> get(ItemIO<Item> io, Long key)
    {
        BinRecord record = getRecord(key);
        return record == null ? null : unmarshall(io, record);
    }

    Box<Item> get(ItemIO<Item> io, Long key, Long version)
    {
        BinRecord record = getRecord(key, version);
        return record == null ? null : unmarshall(io, record);
    }

    // FIXME Call this somewhere somehow.
    void copacetic()
    {
        Set<Long> seen = new HashSet<Long>();
        Cursor<BinRecord> isolated = isolation.first();
        while (isolated.hasNext())
        {
            BinRecord record = isolated.next();
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
            throw new Error("Concurrent modification.", Depot.CONCURRENT_MODIFICATION_ERROR);
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

    public BinCursor first(Unmarshaller unmarshaller)
    {
        return new BinCursor(snapshot, mutator, isolation.first(), schema.getStrata().query(mutator).first(), unmarshaller);
    }
}
