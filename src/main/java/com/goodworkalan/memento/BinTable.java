package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.pack.Mutator;

// TODO Document.
public class BinTable implements Iterable<Ilk.Pair>
{
    // TODO Document.
    private final Map<Ilk.Key, Ilk.Pair> table = new HashMap<Ilk.Key, Ilk.Pair>();
    
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
        Ilk.Pair pair = table.get(ilk.key);
        if (pair == null)
        {
            BinSchema<T> binSchema = binSchemas.get(ilk);
            IndexTable<T> indexes = new IndexTable<T>(this, binSchema.getIndexSchemas());
            pair = binIlk.pair(new Bin<T>(null, snapshot, mutator, binSchemas.get(ilk), indexes, janitors));
            table.put(ilk.key, pair);
        }
        return pair.cast(binIlk);
    }
    
    // TODO Document.
    public Iterator<Ilk.Pair> iterator()
    {
        return table.values().iterator();
    }
}
