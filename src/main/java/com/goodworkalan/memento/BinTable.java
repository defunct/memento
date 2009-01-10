package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.pack.Mutator;

public class BinTable
{
    private final Map<Object, Object> table = new HashMap<Object, Object>();
    
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

    @SuppressWarnings("unchecked")
    public <T> Bin<T> get(Item<T> item)
    {
        Object bin = table.get(item);
        if (bin == null)
        {
            bin = new Bin<T>(snapshot, mutator, binSchemas.get(item), janitors);
            table.put(item, bin);
        }
        return (Bin<T>) bin;
    }   
}
