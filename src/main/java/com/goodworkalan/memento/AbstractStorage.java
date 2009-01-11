package com.goodworkalan.memento;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.fossil.FossilStorage;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Construction;
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
    
    private final Map<Map<Item<?>, Index<?>>, IndexStorage<?>> mapOfIndexStorage = new HashMap<Map<Item<?>, Index<?>>, IndexStorage<?>>();
    
    private final Map<Link, JoinStorage> mapOfJoinStorage = new HashMap<Link, JoinStorage>();
    
    protected abstract StrataPointer open(A address);

    protected abstract StrataPointer create();
    
    protected abstract A record(StrataPointer pointer, long address, Mutator mutator);
   
    public Strata<SnapshotRecord, Long> getSnapshots()
    {
        return snapshots.getStrata();
    }
    
    public Query<SnapshotRecord, Long> newSnapshotQuery()
    {
        Mutator mutator = snapshots.getPack().mutate();
        Query<SnapshotRecord, Long> query = snapshots.getStrata().query(Fossil.initialize(new Stash(), mutator));
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
                
                Schema<BinRecord, Long> schema = Fossil.newFossilSchema();
                schema.setExtractor(new BinExtractor());
                schema.setFieldCaching(true);
                
                Construction<BinRecord, Long, Long> newStrata = schema.create(Fossil.initialize(new Stash(), mutator), new FossilStorage<BinRecord, Long>(new BinRecordIO()));
                
                mapOfBins.put(item, record(pointer, newStrata.getAddress(), mutator));
                
                Query<BinRecord, Long> query = newStrata.getQuery();
                query.flush();
                
                mapOfBinStorage.put(item, new BinStorage(pack, query.getStrata()));
    
                mutator.commit();
            }
            storage = mapOfBinStorage.get(address);
            if (storage == null)
            {
                StrataPointer pointer = open(address);
                
                Schema<BinRecord, Long> schema = Fossil.newFossilSchema();
                schema.setExtractor(new BinExtractor());
                schema.setFieldCaching(true);
                
                Strata<BinRecord, Long> strata = schema.open(new Stash(), new FossilStorage<BinRecord, Long>(new BinRecordIO()), pointer.getRootAddress());
                storage = new BinStorage(pointer.getPack(), strata);
                
                mapOfBinStorage.put(item, storage);
            }
        }
        return storage;
    }

    public <T, F extends Comparable<F>> IndexStorage<F> open(Item<T> item, Index<F> index)
    {
        IndexStorage<F> storage;
        synchronized (mapOfIndexes)
        {
            Map<Item<?>, Index<?>> key = Collections.<Item<?>, Index<?>>singletonMap(item, index);
            A address = mapOfIndexes.get(key);
            if (address == null)
            {
                StrataPointer pointer = create();
                Pack pack = pointer.getPack();
                
                Mutator mutator = pack.mutate();
                
                Schema<IndexRecord, F> schema = Fossil.newFossilSchema();
                schema.setExtractor(new IndexExtractor<T, F>(item, index));
                schema.setFieldCaching(true);
                
                Construction<IndexRecord, F, Long> newStrata = schema.create(Fossil.initialize(new Stash(), mutator), new FossilStorage<IndexRecord, F>(new IndexRecordIO()));
                
                mapOfIndexes.put(key, record(pointer, newStrata.getAddress(), mutator));
                
                Query<IndexRecord, F> query = newStrata.getQuery();
                query.flush();
                
                mapOfIndexStorage.put(key, new IndexStorage<F>(pack, query.getStrata()));

                mutator.commit();
            }
            storage = new UnsafeCast<IndexStorage<F>>().cast(mapOfIndexStorage.get(key));
            if (storage == null)
            {
                StrataPointer pointer = open(address);
                Pack pack = pointer.getPack();
                long rootAddress = pointer.getRootAddress();
                
                Schema<IndexRecord, F> schema = Fossil.newFossilSchema();
                schema.setExtractor(new IndexExtractor<T, F>(item, index));
                schema.setFieldCaching(true);
                
                Strata<IndexRecord, F> strata = schema.open(new Stash(), new FossilStorage<IndexRecord, F>(new IndexRecordIO()), rootAddress);
                storage = new IndexStorage<F>(pack, strata);
                
                mapOfIndexStorage.put(key, storage);
            }
        }
        return storage;
    }
 
    public JoinStorage open(Link link)
    {
        JoinStorage storage;
        synchronized (mapOfJoins)
        {
            A address = mapOfJoins.get(link);
            if (address == null)
            {
                StrataPointer pointer = create();
                Pack pack = pointer.getPack();
                
                Mutator mutator = pack.mutate();
                
                Schema<JoinRecord, Ordered> schema = Fossil.newFossilSchema();
                schema.setExtractor(new JoinExtractor());
                schema.setFieldCaching(true);
                
                Construction<JoinRecord, Ordered, Long> newStrata = schema.create(Fossil.initialize(new Stash(), mutator), new FossilStorage<JoinRecord, Ordered>(new JoinRecordIO(link.size())));
                
                mapOfJoins.put(link, record(pointer, newStrata.getAddress(), mutator));
                
                Query<JoinRecord, Ordered> query = newStrata.getQuery();
                query.flush();
                
                mapOfJoinStorage.put(link, new JoinStorage(pack, query.getStrata()));

                mutator.commit();
            }
            storage = mapOfJoinStorage.get(link);
            if (storage == null)
            {
                StrataPointer pointer = open(address);
                Pack pack = pointer.getPack();
                long rootAddress = pointer.getRootAddress();
                
                Schema<JoinRecord, Ordered> schema = Fossil.newFossilSchema();
                schema.setExtractor(new JoinExtractor());
                schema.setFieldCaching(true);
                
                Strata<JoinRecord, Ordered> strata = schema.open(new Stash(), new FossilStorage<JoinRecord, Ordered>(new JoinRecordIO(link.size())), rootAddress);
                storage = new JoinStorage(pack, strata);
                
                mapOfJoinStorage.put(link, storage);
            }
        }
        return storage;
    }
}
