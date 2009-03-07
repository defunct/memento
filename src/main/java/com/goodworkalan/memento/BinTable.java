package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.goodworkalan.pack.Mutator;

public class BinTable implements Iterable<Bin<?>>
{
    private final Map<Item<?>, Bin<?>> table = new HashMap<Item<?>, Bin<?>>();
    
    private final Snapshot snapshot;
    
    private final Mutator mutator;
    
    private final BinSchemaTable binSchemas;
    
    private final Map<Long, Janitor> janitors;
    
    public BinTable(Snapshot snapshot, Mutator mutator, BinSchemaTable binSchemas, Map<Long, Janitor> janitors)
    {
        this.snapshot = snapshot;
        this.mutator = mutator;
        this.binSchemas = binSchemas;
        this.janitors = janitors;
    }

    public <T> Bin<T> get(Item<T> item)
    {
        Bin<?> bin = table.get(item);
        if (bin == null)
        {
            BinSchema<T> binSchema = binSchemas.get(item);
            IndexTable<T> indexes = new IndexTable<T>(this, binSchema.getIndexSchemas());
            bin = new Bin<T>(null, snapshot, mutator, binSchemas.get(item), indexes, janitors);
            table.put(item, bin);
        }
        return new UnsafeCast<Bin<T>>().cast(bin);
    }
    
    public Iterator<Bin<?>> iterator()
    {
        return table.values().iterator();
    }
}
