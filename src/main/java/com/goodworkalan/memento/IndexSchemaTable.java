package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;

// TODO Document.
public class IndexSchemaTable<T>
{
    // TODO Document.
    private final Map<Index<?>, Ilk.Pair> table = new HashMap<Index<?>, Ilk.Pair>();
    
    // TODO Document.
    private final BinSchema<T> binSchema;
    
    // TODO Document.
    public IndexSchemaTable(BinSchema<T> binSchema)
    {
        this.binSchema = binSchema;
    }

    // TODO Document.
    public <F extends Comparable<? super F>> IndexSchema<T, F> get(Index<F> index)
    {
        Ilk<IndexSchema<T, F>> indexSchemaIlk = new Ilk<IndexSchema<T,F>>(binSchema.getIlk().key, index.getIlk().key) { };
        Ilk.Pair pair = table.get(index);
        if (pair == null)
        {
            pair = indexSchemaIlk.pair(new IndexSchema<T, F>(binSchema.getIlk(), index));
            table.put(index, pair);
        }
        return pair.cast(indexSchemaIlk);
    }
}
