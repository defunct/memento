package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;


public class IndexTable<T>
{
    private final Map<Object, Object> table = new HashMap<Object, Object>();
    
    private final IndexSchemaTable<T> indexSchemas;
    
    public IndexTable(IndexSchemaTable<T> indexSchemas)
    {
        this.indexSchemas = indexSchemas;
    }
    
    public <F> IndexMutator<T, F> get(Index<F> index)
    {
        Object object = table.get(index);
        if (object == null)
        {
            object = new IndexMutator<T, F>(indexSchemas.get(index));
            table.put(index, object);
        }
        return new UnsafeCast<IndexMutator<T, F>>().cast(object);
    }
}
