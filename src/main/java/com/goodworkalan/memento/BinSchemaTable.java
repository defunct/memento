package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

public class BinSchemaTable
{
    private final Map<Object, Object> table = new HashMap<Object, Object>();
    
    public <T> BinSchema<T> get(Item<T> item)
    {
        Object object = table.get(item);
        if (object == null)
        {
            BinSchema<T> binSchema = new BinSchema<T>(item);
            binSchema.setItemIO(SerializationIO.getInstance(item.caster));
            table.put(item, binSchema);
        }
        return new UnsafeCast<BinSchema<T>>().cast(object);
    }
    
    public boolean has(Item<?> item)
    {
        return table.containsKey(item);
    }
}
