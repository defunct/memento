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
    
    private final BinTable bins;
    
    public BinTable(BinTable bins, Snapshot snapshot, Mutator mutator, BinSchemaTable binSchemas, Map<Long, Janitor> janitors)
    {
        this.bins = bins;
        this.snapshot = snapshot;
        this.mutator = mutator;
        this.binSchemas = binSchemas;
        this.janitors = janitors;
    }

    public <T> Bin<T> get(Item<T> item)
    {
        Object bin = table.get(item);
        if (bin == null)
        {
            BinSchema<T> binSchema = binSchemas.get(item);
            IndexTable<T> indexes = new IndexTable<T>(bins, binSchema.getIndexSchemas());
            bin = new Bin<T>(snapshot, mutator, binSchemas.get(item), indexes, janitors);
            table.put(item, bin);
        }
        return new UnsafeCast<Bin<T>>().cast(bin);
    }   
}
