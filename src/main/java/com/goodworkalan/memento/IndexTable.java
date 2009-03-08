package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.goodworkalan.ilk.UncheckedCast;


public class IndexTable<T> implements Iterable<IndexMutator<T, ?>>
{
    private final Map<Index<?>, IndexMutator<T, ?>> table = new HashMap<Index<?>, IndexMutator<T, ?>>();
    
    private final BinTable bins;
    
    private final IndexSchemaTable<T> indexSchemas;
    
    public IndexTable(BinTable bins, IndexSchemaTable<T> indexSchemas)
    {
        this.bins = bins;
        this.indexSchemas = indexSchemas;
    }
    
    public <F extends Comparable<F>> IndexMutator<T, F> get(Index<F> index)
    {
        IndexMutator<T, ?> object = table.get(index);
        if (object == null)
        {
            object = new IndexMutator<T, F>(bins, indexSchemas.get(index));
            table.put(index, object);
        }
        return new UncheckedCast<IndexMutator<T, F>>().cast(object);
    }
    
    public Iterator<IndexMutator<T, ?>> iterator()
    {
        return table.values().iterator();
    }
}
