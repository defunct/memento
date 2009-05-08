package com.goodworkalan.memento;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.pack.io.ByteBufferInputStream;
import com.goodworkalan.pack.io.PackOutputStream;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;

// FIXME Vacuum.
// TODO Document.
public final class Bin<T>
{
    // TODO Document.
    final Mutator mutator;

    // TODO Document.
    private final Snapshot snapshot;

    // TODO Document.
    private final BinSchema<T> binSchema;

    // TODO Document.
    private final IndexTable<T> indexes;
    
    // TODO Document.
    final Query<BinRecord> query;

    // TODO Document.
    private final Query<BinRecord> isolation;
    
    // TODO Document.
    private final WeakIdentityLookup outstandingKeys;
    
    // TODO Document.
    private final WeakHashMap<Long, Box<T>> outstandingValues; 

    // TODO Document.
    public Bin(PackFactory storage, Snapshot snapshot, Mutator mutator, BinSchema<T> schema, IndexTable<T> indexes, Map<Long, Janitor> janitors)
    {
//        BinStorage binStorage = storage.open(schema.getItem());
        query = schema.getStrata().query(Fossil.newStash(mutator));
        isolation = new BinTree().create(mutator);
        BinJanitor<T> janitor = new BinJanitor<T>(isolation, schema.getIlk());

        PackOutputStream allocation = new PackOutputStream(mutator);
        try
        {
            ObjectOutputStream out = new ObjectOutputStream(allocation);
            out.writeObject(janitor);
        }
        catch (IOException e)
        {
            throw new MementoException(104, e);
        }

        long address = allocation.allocate();
        mutator.setTemporary(address);
        janitors.put(address, janitor);

        this.snapshot = snapshot;
        this.indexes = indexes;
        this.binSchema = schema;
        this.mutator = mutator;
        
        this.outstandingKeys = new WeakIdentityLookup();
        this.outstandingValues = new WeakHashMap<Long, Box<T>>();
    }
    
    // TODO Document.
    public List<T> getAll()
    {
        return Collections.emptyList();
    }
    
    // TODO Document.
    BinSchema<T> getBinSchema()
    {
        return binSchema;
    }
    
    // TODO Document.
    IndexTable<T> getIndexes()
    {
        return indexes;
    }

    // TODO Document.
    public void restore(long key, T object)
    {
        Box<T> box = new Box<T>(key, snapshot.getVersion(), object);
        insert(box);
        binSchema.setIdentifierIf(key);
    }

    // TODO Document.
    private void insert(Box<T> box)
    {
        PackOutputStream allocation = new PackOutputStream(mutator);

        binSchema.getItemIO().write(allocation, box.getItem());
 
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

    // TODO Document.
    public long add(T item)
    {
        Box<T> box = new Box<T>(binSchema.nextIdentifier(), snapshot.getVersion(), item);
        
        insert(box);

        return box.getKey();
    }

    // TODO Document.
    private static boolean isDeleted(BinRecord record)
    {
        return record.address == Pack.NULL_ADDRESS;
    }

    // TODO Document.
    private BinRecord record(Long key)
    {
        BinRecord record = isolation.remove(new BinKeyComparable(key));
        if (record != null)
        {
            mutator.free(record.address);
        }
        else
        {
            record = get(query.find(new BinKeyComparable(key)), key, false);
        }
        if (record != null)
        {
            record = isDeleted(record) ? null : record;
        }
        return record;
    }
    
    // TODO Document.
    public long update(T item)
    {
        Long key = outstandingKeys.get(item);

        if (key == null)
        {
            throw new MementoException(105);
        }
        
        return update(key, item).getKey();
    }
    
    // TODO Document.
    public long replace(T then, T now)
    {
        Long key = outstandingKeys.get(then);
        
        if (key == null)
        {
            throw new MementoException(106);
        }
        
        return update(key, now).getKey();
    }

    // TODO Document.
    public Box<T> update(long key, T item)
    {
        BinRecord record = record(key);

        if (record == null)
        {
            throw new MementoException(105);
        }

        PackOutputStream allocation = new PackOutputStream(mutator);
        binSchema.getItemIO().write(allocation, item);
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

    // TODO Document.
    public void delete(T item)
    {
        long key = key(item);
        if (key == 0L)
        {
            throw new IllegalArgumentException();
        }
        delete(key);
    }

    // TODO Document.
    public void delete(long key)
    {
        BinRecord record = record(key);

        if (record == null)
        {
            throw new MementoException(107);
        }

        if (record.version != snapshot.getVersion())
        {
            isolation.add(new BinRecord(key, snapshot.getVersion(), Pack.NULL_ADDRESS));
        }
    }

    // TODO Document.
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

    // TODO Document.
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

    // TODO Document.
    private Box<T> unmarshall(BinRecord record)
    {
        ByteBuffer block = mutator.read(record.address);
        T item = binSchema.getItemIO().read(new ByteBufferInputStream(block));
        return new Box<T>(record.key, record.version, item);
    }

    // TODO Document.
    private BinRecord getRecord(Long key)
    {
        BinRecord stored = get(query.find(new BinKeyComparable(key)), key, false);
        BinRecord isolated = get(isolation.find(new BinKeyComparable(key)), key, true);
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

    // TODO Document.
    private BinRecord getRecord(Long key, Long version)
    {
        BinRecord stored = getVersion(query.find(new BinKeyComparable(key)), key, version);
        BinRecord isolated = getVersion(isolation.find(new BinKeyComparable(key)), key, version);
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

    // TODO Document.
    public T get(long key)
    { 
        Box<T> box = box(key);
        if (box == null)
        {
            return null;
        }
        return box.getItem();
    }

    // TODO Document.
    public long key(T item)
    {
        Long key = outstandingKeys.get(item);
        if (key == null)
        {
            throw new UnsupportedOperationException();
        }
        return key;
    }

    // TODO Document.
    public Box<T> box(long key)
    {
        Box<T> box = outstandingValues.get(key);
        if (box == null)
        {
            box = readBox(key);
            if (box != null)
            {
                outstandingValues.put(box.getKey(), box);
                outstandingKeys.put(box.getItem(), box.getKey());
            }
        }
        return box;
    }

    // TODO Document.
    private Box<T> readBox(Long key)
    {
        BinRecord record = getRecord(key);
        return record == null ? null : unmarshall(record);
    }

    // TODO Document.
    public Box<T> box(Long key, Long version)
    {
        Box<T> box = box(key);
        if (box.getVersion() != version)
        {
            BinRecord record = getRecord(key, version);
            return record == null ? null : unmarshall(record);
        }
        return box;
    }

    // FIXME Call this somewhere somehow.
    // TODO Document.
    void copacetic()
    {
        Set<Long> seen = new HashSet<Long>();
        Cursor<BinRecord> isolated = isolation.first();
        while (isolated.hasNext())
        {
            BinRecord record = isolated.next();
            if (seen.contains(record))
            {
                throw new MementoException(109);
            }
            seen.add(record.key);
        }
    }

    // TODO Document.
    void flush()
    {
    }

    // TODO Document.
    void commit()
    {
        Cursor<BinRecord> isolated = isolation.first();
        while (isolated.hasNext())
        {
            query.add(isolated.next());
        }

        boolean copacetic = true;
        isolated = isolation.first();
        while (copacetic && isolated.hasNext())
        {
            // FIXME Strata will not stop at hasNext(), need an
            // interator that will check against the key.
            BinRecord record = isolated.next();
            Cursor<BinRecord> cursor = query.find(new BinKeyComparable(record.key));
            while (copacetic && cursor.hasNext())
            {
                BinRecord candidate = cursor.next();
                
                assert candidate.key == record.key;

                // Our version of the record should come before any
                // invisible versions. (This probably can be modified to
                // accept known rollbacks, currently it doesn't.)
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
            throw new MementoException(110);
        }

        for (Ilk.Box box : indexes)
        {
            IndexMutator<T, ?> index = box.cast(new Ilk<IndexMutator<T, ?>>(binSchema.getIlk().key) { });
            index.commit(snapshot, mutator, this);
        }
    }

    // TODO Document.
    public <F extends Comparable<F>> IndexCursor<T, F>  find(Index<F> index, F field)
    {
        return indexes.get(index).find(snapshot, mutator, this, field, false);
    }

    // TODO Document.
    public <F extends Comparable<F>> IndexCursor<T, F> first(Index<F> index)
    {
        IndexMutator<T, F> indexMutator = indexes.get(index);
        if (indexMutator == null)
        {
            throw new MementoException(108);
        }

        return indexMutator.first(snapshot, mutator, this);
    }

    // TODO Document.
    public BinCursor<T> first()
    {
        return new BinCursor<T>(snapshot, mutator, isolation.first(), binSchema.getStrata().query(Fossil.initialize(new Stash(), mutator)).first(), binSchema.getItemIO());
    }
    
    // TODO Document.
    public <O> JoinAdd<O> join(T object, Class<O> other)
    {
        return new JoinAdd<O>(snapshot.join(new Link().bin(binSchema.getIlk()).bin(other)), new Ilk<O>(other));
    }
}
