package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.ilk.UncheckedCast;
import com.goodworkalan.pack.Mutator;

public class BinTable implements Iterable<Bin<?>>
{
    private final Map<Ilk.Key, Bin<?>> table = new HashMap<Ilk.Key, Bin<?>>();
    
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

    public <T> Bin<T> get(Ilk<T> ilk)
    {
        Bin<?> bin = table.get(ilk.key);
        if (bin == null)
        {
            BinSchema<T> binSchema = binSchemas.get(ilk);
            IndexTable<T> indexes = new IndexTable<T>(this, binSchema.getIndexSchemas());
            bin = new Bin<T>(null, snapshot, mutator, binSchemas.get(ilk), indexes, janitors);
            table.put(ilk.key, bin);
        }
        return new UncheckedCast<Bin<T>>().cast(bin);
    }
    
    public Iterator<Bin<?>> iterator()
    {
        return table.values().iterator();
    }
}
