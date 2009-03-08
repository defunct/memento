package com.goodworkalan.memento;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.fossil.FossilAllocator;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.ExtractorComparableFactory;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.Strata;

public abstract class AbstractStorage<A> implements Storage
{
    protected SnapshotStorage snapshots;
    
    protected final static URI HEADER_URI = URI.create("http://goodworkalan.com/memento/file-storage-header");
    
    protected final Map<Item<?>, A> mapOfBins = new HashMap<Item<?>, A>();  
    
    protected final Map<Map<Item<?>, Index<?>>, A> mapOfIndexes = new HashMap<Map<Item<?>, Index<?>>, A>();  

    protected final Map<Link, A> mapOfJoins = new HashMap<Link, A>();
    
    private final Map<Item<?>, BinStorage> mapOfBinStorage = new HashMap<Item<?>, BinStorage>();
    
    private final Map<Map<Item<?>, Index<?>>, IndexStorage> mapOfIndexStorage = new HashMap<Map<Item<?>, Index<?>>, IndexStorage>();
    
    private final Map<Link, JoinStorage> mapOfJoinStorage = new HashMap<Link, JoinStorage>();
    
    protected abstract StrataPointer open(A address);

    protected abstract StrataPointer create();
    
    protected abstract A record(StrataPointer pointer, long address, Mutator mutator);
   
    public Strata<SnapshotRecord> getSnapshots()
    {
        return snapshots.getStrata();
    }
    
    public Query<SnapshotRecord> newSnapshotQuery()
    {
        Mutator mutator = snapshots.getPack().mutate();
        Query<SnapshotRecord> query = snapshots.getStrata().query(Fossil.newStash(mutator));
        query.getStash().put(MUTATOR, Mutator.class, mutator);
        return query;
    }

    public <T> BinStorage open(Item<T> item)
    {
        BinStorage storage;
        synchronized (mapOfBins)
        {
            A address = mapOfBins.get(item); 
            if (address == null)
            {
                StrataPointer pointer = create();
                Pack pack = pointer.getPack();
                
                Mutator mutator = pack.mutate();
                
                Schema<BinRecord> schema = new Schema<BinRecord>();
                schema.setInnerCapacity(7);
                schema.setLeafCapacity(7);
                schema.setComparableFactory(new ExtractorComparableFactory<BinRecord, Long>(new BinExtractor()));

                Stash stash = Fossil.newStash(mutator);
                long root = schema.create(Fossil.newStash(mutator), new FossilAllocator<BinRecord>(new BinRecordIO()));
                
                mapOfBins.put(item, record(pointer, root, mutator));
                
                mapOfBinStorage.put(item, new BinStorage(pack, schema.open(stash, root, new FossilAllocator<BinRecord>(new BinRecordIO()))));
    
                mutator.commit();
            }
            storage = mapOfBinStorage.get(address);
            if (storage == null)
            {
                StrataPointer pointer = open(address);
                
                Schema<BinRecord> schema = new Schema<BinRecord>();

                schema.setInnerCapacity(7);
                schema.setLeafCapacity(7);
                schema.setComparableFactory(new ExtractorComparableFactory<BinRecord, Long>(new BinExtractor()));
                
                
                Strata<BinRecord> strata = schema.open(new Stash(), pointer.getRootAddress(), new FossilAllocator<BinRecord>(new BinRecordIO()));
                storage = new BinStorage(pointer.getPack(), strata);
                
                mapOfBinStorage.put(item, storage);
            }
        }
        return storage;
    }

    public <T, F extends Comparable<F>> IndexStorage open(Item<T> item, Index<F> index)
    {
        FossilAllocator<IndexRecord> fossilStorage = new FossilAllocator<IndexRecord>(new IndexRecordIO());

        IndexStorage storage;
        synchronized (mapOfIndexes)
        {
            Map<Item<?>, Index<?>> key = Collections.<Item<?>, Index<?>>singletonMap(item, index);
            A address = mapOfIndexes.get(key);
            if (address == null)
            {
                StrataPointer pointer = create();
                Pack pack = pointer.getPack();
                
                Mutator mutator = pack.mutate();
                
                Schema<IndexRecord> schema = new Schema<IndexRecord>();
                
                schema.setInnerCapacity(7);
                schema.setLeafCapacity(7);
                schema.setComparableFactory(new ExtractorComparableFactory<IndexRecord, F>(new IndexExtractor<T, F>(item, index)));
                
                Stash stash = Fossil.newStash(mutator);
                long rootAddress = schema.create(stash, fossilStorage);
                
                mapOfIndexes.put(key, record(pointer, rootAddress, mutator));
                
                mapOfIndexStorage.put(key, new IndexStorage(pack, schema.open(stash, rootAddress, fossilStorage)));

                mutator.commit();
            }
            storage = (IndexStorage) mapOfIndexStorage.get(key);
            if (storage == null)
            {
                StrataPointer pointer = open(address);
                Pack pack = pointer.getPack();
                long rootAddress = pointer.getRootAddress();
                
                Schema<IndexRecord> schema = new Schema<IndexRecord>();
                
                schema.setInnerCapacity(7);
                schema.setLeafCapacity(7);
                schema.setComparableFactory(new ExtractorComparableFactory<IndexRecord, F>(new IndexExtractor<T, F>(item, index)));
                
                Strata<IndexRecord> strata = schema.open(new Stash(), rootAddress, fossilStorage);
                storage = new IndexStorage(pack, strata);
                
                mapOfIndexStorage.put(key, storage);
            }
        }
        return storage;
    }
 
    public JoinStorage open(Link link)
    {
        Schema<JoinRecord> schema = new Schema<JoinRecord>();
        
        schema.setInnerCapacity(7);
        schema.setLeafCapacity(7);
        schema.setComparableFactory(new ExtractorComparableFactory<JoinRecord, KeyList>(new JoinExtractor()));

        FossilAllocator<JoinRecord> fossilStorage = new FossilAllocator<JoinRecord>(new JoinRecordIO(link.size()));

        JoinStorage storage;
        synchronized (mapOfJoins)
        {
            A address = mapOfJoins.get(link);
            if (address == null)
            {
                StrataPointer pointer = create();
                Pack pack = pointer.getPack();
                
                Mutator mutator = pack.mutate();
                
                Stash stash = Fossil.newStash(mutator);
                long rootAddress = schema.create(stash, fossilStorage);
                
                mapOfJoins.put(link, record(pointer, rootAddress, mutator));
                
                mapOfJoinStorage.put(link, new JoinStorage(pack, schema.open(stash, rootAddress, fossilStorage)));

                mutator.commit();
            }
            storage = mapOfJoinStorage.get(link);
            if (storage == null)
            {
                StrataPointer pointer = open(address);
                Pack pack = pointer.getPack();
                long rootAddress = pointer.getRootAddress();
                
                Strata<JoinRecord> strata = schema.open(new Stash(), rootAddress, fossilStorage);
                storage = new JoinStorage(pack, strata);
                
                mapOfJoinStorage.put(link, storage);
            }
        }
        return storage;
    }
}
