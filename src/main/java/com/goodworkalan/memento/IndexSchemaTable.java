package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

public class IndexSchemaTable<T>
{
    private final Map<Object, Object> table = new HashMap<Object, Object>();
    
    private final BinSchemaTable binSchemas;
    
    private final Item<T> item;
    
    public IndexSchemaTable(BinSchemaTable binSchemas, Item<T> item)
    {
        this.binSchemas = binSchemas;
        this.item = item;
    }

    public <F> IndexSchema<T, F> get(Index<F> index)
    {
        Object object = table.get(index);
        if (object == null)
        {
            ItemIO<T> io = binSchemas.get(item).getItemIO();
            IndexSchema<T, F> indexSchema = new IndexSchema<T, F>();
            indexSchema.setItemIO(io);
            
            object = indexSchema;
            table.put(index, object);
        }
        return new UnsafeCast<IndexSchema<T, F>>().cast(object); 
    }
}
