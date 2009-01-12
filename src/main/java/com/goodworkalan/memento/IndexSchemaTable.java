package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

public class IndexSchemaTable<T>
{
    private final Map<Object, IndexSchema<T, ?>> table = new HashMap<Object, IndexSchema<T, ?>>();
    
    private final BinSchema<T> binSchema;
    
    public IndexSchemaTable(BinSchema<T> binSchema)
    {
        this.binSchema = binSchema;
    }

    public <F> IndexSchema<T, F> get(Index<F> index)
    {
        IndexSchema<T, ?> indexSchema = table.get(index);
        if (indexSchema == null)
        {
            indexSchema = new IndexSchema<T, F>(binSchema.getItem(), index);
            table.put(index, indexSchema);
        }
        return new UnsafeCast<IndexSchema<T, F>>().cast(indexSchema); 
    }
}
