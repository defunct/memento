package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

public class IndexSchemaTable<T>
{
    private final Map<Object, Object> table = new HashMap<Object, Object>();
    
    private final BinSchema<T> binSchema;
    
    public IndexSchemaTable(BinSchema<T> binSchema)
    {
        this.binSchema = binSchema;
    }

    public <F> IndexSchema<T, F> get(Index<F> index)
    {
        Object object = table.get(index);
        if (object == null)
        {
            IndexSchema<T, F> indexSchema = new IndexSchema<T, F>(binSchema.getItem(), index);
            indexSchema.setItemIO(binSchema.getItemIO());
            
            object = indexSchema;
            table.put(index, object);
        }
        return new UnsafeCast<IndexSchema<T, F>>().cast(object); 
    }
}
