package com.goodworkalan.memento;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;

// FIXME Vacuum.
public final class Bin<T>
{
    private final Item<T> item;
    
    final Mutator mutator;

    private final Snapshot snapshot;

    private final BinSchema<T> schema;

    private final IndexTable<T> indexes;
    
    final Query<BinRecord, Long> query;

    private final Query<BinRecord, Long> isolation;
    
    private final WeakIdentityLookup outstandingKeys;
    
    private final WeakHashMap<Long, Box<T>> outstandingValues; 

    public Bin(Storage storage,
               Snapshot snapshot,
               Mutator mutator,
               BinSchema<T> schema,
               IndexTable<T> indexes,
               Map<Long, Janitor> janitors)
    {
        BinStorage binStorage = storage.open(item);
        query = schema.getStrata().query(Fossil.initialize(new Stash(), mutator));
        isolation = new BinTree().create(mutator);
        BinJanitor<T> janitor = new BinJanitor<T>(isolation, itemClass);

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
        janitors.put(address, janitor);

        this.snapshot = snapshot;
        this.indexes = indexes;
        this.schema = schema;
        this.mutator = mutator;
        
        this.outstandingKeys = new WeakIdentityLookup();
        this.outstandingValues = new WeakHashMap<Long, Box<T>>();
    }
    
    public List<T> getAll()
    {
        return Collections.emptyList();
    }
    
    BinSchema<T> getSchema()
    {
        return schema;
    }
    
    IndexTable<T> getIndexes()
    {
        return indexes;
    }

    public void restore(long key, T object)
    {
        Box<T> box = new Box<T>(key, snapshot.getVersion(), object);
        insert(box);
        schema.setIdentifierIf(key);
    }

    private void insert(Box<T> box)
    {
        PackOutputStream allocation = new PackOutputStream(mutator);

        schema.getItemIO().write(allocation, box.getItem());
 
        long address = allocation.allocate();
        BinRecord record = new BinRecord(box.getKey(), box.getVersion(), address);
        isolation.add(record);

//        for (Map.Entry<String, Index> entry : mapOfIndices.entrySet())
//        {
//            try
//            {
//                entry.getValue().add(snapshot, mutator, this, bag);
//            }
//            catch (Error e)
//            {
//                e.put("index", entry.getKey());
//                isolation.remove(isolation.extract(record));
//                mutator.free(record.address);
//                throw e;
//            }
//        }
    }

    public long add(T item)
    {
        Box<T> box = new Box<T>(schema.nextIdentifier(), snapshot.getVersion(), item);
        
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
            record = get(query.find(key), key, false);
        }
        if (record != null)
        {
            record = isDeleted(record) ? null : record;
        }
        return record;
    }
    
    public long update(T item)
    {
        Long key = outstandingKeys.get(item);

        if (key == null)
        {
            throw new Danger("update.bag.does.not.exist", 401);
        }
        
        return update(key, item).getKey();
    }
    
    public long replace(T then, T now)
    {
        Long key = outstandingKeys.get(then);
        
        if (key == null)
        {
            throw new Danger("error", 401);
        }
        
        return update(key, now).getKey();
    }

    public Box<T> update(long key, T item)
    {
        BinRecord record = record(key);

        if (record == null)
        {
            throw new Danger("update.bag.does.not.exist", 401);
        }

        PackOutputStream allocation = new PackOutputStream(mutator);
        schema.getItemIO().write(allocation, item);
        long address = allocation.allocate();
        
        Box<T> box = new Box<T>(key, snapshot.getVersion(), item);
        isolation.add(new BinRecord(box.getKey(), box.getVersion(), address));

//        Iterator<Index> indices = mapOfIndices.values().iterator();
//        while (indices.hasNext())
//        {
//            Index index = (Index) indices.next();
//            index.update(snapshot, mutator, this, bag, record.version);
//        }

        return box;
    }

    public void delete(T item)
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

    private Box<T> unmarshall(ItemIO<T> io, BinRecord record)
    {
        ByteBuffer block = mutator.read(record.address);
        T item = io.read(new ByteBufferInputStream(block));
        return new Box<T>(record.key, record.version, item);
    }

    private BinRecord getRecord(Long key)
    {
        BinRecord stored = get(query.find(key), key, false);
        BinRecord isolated = get(isolation.find(key), key, true);
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
        BinRecord stored = getVersion(query.find(key), key, version);
        BinRecord isolated = getVersion(isolation.find(key), key, version);
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

    public T get(long key)
    { 
        Box<T> box = box(key);
        if (box == null)
        {
            return null;
        }
        return box.getItem();
    }

    public long key(T item)
    {
        Long key = outstandingKeys.get(item);
        if (key == null)
        {
            throw new UnsupportedOperationException();
        }
        return key;
    }

    public Box<T> box(long key)
    {
        Box<T> box = outstandingValues.get(key);
        if (box == null)
        {
            box = get(schema.getItemIO(), key);
            outstandingValues.put(box.getKey(), box);
            outstandingKeys.put(box.getItem(), box.getKey());
        }
        return box;
    }

    public Box<T> get(ItemIO<T> io, Long key)
    {
        BinRecord record = getRecord(key);
        return record == null ? null : unmarshall(io, record);
    }

    Box<T> get(ItemIO<T> io, Long key, Long version)
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

    public <F extends Comparable<F>> IndexCursor first(Index<F> index)
    {
        IndexMutator<T, F> indexMutator = indexes.get(index);
        if (indexMutator == null)
        {
            throw new Danger("no.such.index", 503).add(index);
        }

        return indexMutator.first(snapshot, mutator, this);
    }

    public IndexCursor first()
    {
        return first(schema.schema.unmarshaller);
    }

    public BinCursor first(Unmarshaller unmarshaller)
    {
        return new BinCursor(snapshot, mutator, isolation.first(), schema.getStrata().query(mutator).first(), unmarshaller);
    }
    
    public <O> JoinAdd<O> join(T object, Class<O> other)
    {
        return new JoinAdd<O>(snapshot.join(new Link().bin(item).bin(other)), new Item<O>(other) {});
    }
}
