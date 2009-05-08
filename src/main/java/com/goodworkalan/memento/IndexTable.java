package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;

// TODO Document.
public class IndexTable<T> implements Iterable<Ilk.Box>
{
    // TODO Document.
    private final Map<Index<?>, Ilk.Box> table = new HashMap<Index<?>, Ilk.Box>();
    
    // TODO Document.
    private final BinTable bins;
    
    // TODO Document.
    private final IndexSchemaTable<T> indexSchemas;
    
    // TODO Document.
    public IndexTable(BinTable bins, IndexSchemaTable<T> indexSchemas)
    {
        this.bins = bins;
        this.indexSchemas = indexSchemas;
    }
    
    // TODO Document.
    public <F extends Comparable<F>> IndexMutator<T, F> get(Index<F> index)
    {
        IndexSchema<T, F> indexSchema = indexSchemas.get(index);
        
        Ilk<IndexMutator<T, F>> indexMutatorIlk = new Ilk<IndexMutator<T,F>>(indexSchema.getIlk().key, index.getIlk().key) { };
        Ilk.Box pair = table.get(index);
        if (pair == null)
        {
            pair = indexMutatorIlk.box(new IndexMutator<T, F>(bins, indexSchemas.get(index)));
            table.put(index, pair);
        }
        return pair.cast(indexMutatorIlk);
    }
    
    // TODO Document.
    public Iterator<Ilk.Box> iterator()
    {
        return table.values().iterator();
    }
}
