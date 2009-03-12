package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;

// TODO Document.
public class BinSchemaTable
{
    // TODO Document.
    private final Map<Ilk.Key, Ilk.Pair> table = new HashMap<Ilk.Key, Ilk.Pair>();
    
    // TODO Document.
    public <T> BinSchema<T> get(Ilk<T> ilk)
    {
        Ilk<BinSchema<T>> schemaIlk = new Ilk<BinSchema<T>>(ilk.key) { };
        
        Ilk.Pair pair = table.get(ilk.key);
        if (pair == null)
        {
            BinSchema<T> binSchema = new BinSchema<T>(ilk);
            binSchema.setItemIO(SerializationIO.getInstance(ilk));
            table.put(ilk.key, schemaIlk.pair(binSchema));
        }
        return pair.cast(schemaIlk);
    }
    
    // TODO Document.
    public boolean has(Ilk.Key key)
    {
        return table.containsKey(key);
    }
}
