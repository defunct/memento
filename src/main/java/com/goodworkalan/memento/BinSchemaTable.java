package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.ilk.UncheckedCast;

public class BinSchemaTable
{
    private final Map<Object, Object> table = new HashMap<Object, Object>();
    
    public <T> BinSchema<T> get(Ilk<T> ilk)
    {
        Object object = table.get(ilk.key);
        if (object == null)
        {
            BinSchema<T> binSchema = new BinSchema<T>(ilk);
            binSchema.setItemIO(SerializationIO.getInstance(ilk));
            table.put(ilk.key, binSchema);
        }
        return new UncheckedCast<BinSchema<T>>().cast(object);
    }
    
    public boolean has(Ilk.Key key)
    {
        return table.containsKey(key);
    }
}
