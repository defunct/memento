package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;


public class IndexTable<T>
{
    private final Map<Object, Object> table = new HashMap<Object, Object>();
    
    private final BinTable bins;
    
    private final IndexSchemaTable<T> indexSchemas;
    
    public IndexTable(BinTable bins, IndexSchemaTable<T> indexSchemas)
    {
        this.bins = bins;
        this.indexSchemas = indexSchemas;
    }
    
    public <F extends Comparable<F>> IndexMutator<T, F> get(Index<F> index)
    {
        Object object = table.get(index);
        if (object == null)
        {
            object = new IndexMutator<T, F>(bins, indexSchemas.get(index));
            table.put(index, object);
        }
        return new UnsafeCast<IndexMutator<T, F>>().cast(object);
    }
}
