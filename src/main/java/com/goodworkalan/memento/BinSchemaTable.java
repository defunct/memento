package com.goodworkalan.memento;

import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;

// TODO Document.
public class BinSchemaTable
{
    // TODO Document.
    private final Map<Ilk.Key, Ilk.Box> table = new HashMap<Ilk.Key, Ilk.Box>();
    
    // TODO Document.
    public <T> BinSchema<T> get(Ilk<T> ilk)
    {
        Ilk<BinSchema<T>> schemaIlk = new Ilk<BinSchema<T>>() { }.assign((TypeVariable<?>) new Ilk<T>() {}.key.type, ilk.key.type);
        
        Ilk.Box box = table.get(ilk.key);
        if (box == null)
        {
            BinSchema<T> binSchema = new BinSchema<T>(ilk);
            binSchema.setItemIO(SerializationIO.getInstance(ilk));
            table.put(ilk.key, schemaIlk.box(binSchema));
        }
        return box.cast(schemaIlk);
    }
    
    // TODO Document.
    public boolean has(Ilk.Key key)
    {
        return table.containsKey(key);
    }
}
