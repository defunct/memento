package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.pack.Mutator;

// TODO Document.
public class BinTable implements Iterable<Ilk.Box>
{
    // TODO Document.
    private final Map<Ilk.Key, Ilk.Box> table = new HashMap<Ilk.Key, Ilk.Box>();
    
    // TODO Document.
    private final Snapshot snapshot;
    
    // TODO Document.
    private final Mutator mutator;
    
    // TODO Document.
    private final BinSchemaTable binSchemas;
    
    // TODO Document.
    private final Map<Long, Janitor> janitors;
    
    // TODO Document.
    public BinTable(Snapshot snapshot, Mutator mutator, BinSchemaTable binSchemas, Map<Long, Janitor> janitors)
    {
        this.snapshot = snapshot;
        this.mutator = mutator;
        this.binSchemas = binSchemas;
        this.janitors = janitors;
    }

    // TODO Document.
    public <T> Bin<T> get(Ilk<T> ilk)
    {
        Ilk<Bin<T>> binIlk = new Ilk<Bin<T>>(ilk.key) { };
        Ilk.Box box = table.get(ilk.key);
        if (box == null)
        {
            BinSchema<T> binSchema = binSchemas.get(ilk);
            IndexTable<T> indexes = new IndexTable<T>(this, binSchema.getIndexSchemas());
            box = binIlk.box(new Bin<T>(null, snapshot, mutator, binSchemas.get(ilk), indexes, janitors));
            table.put(ilk.key, box);
        }
        return box.cast(binIlk);
    }
    
    // TODO Document.
    public Iterator<Ilk.Box> iterator()
    {
        return table.values().iterator();
    }
}
