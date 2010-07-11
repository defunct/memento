package com.goodworkalan.memento;

import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;

// TODO Document.
public class IndexSchemaTable<T>
{
    // TODO Document.
    private final Map<Index<?>, Ilk.Box> table = new HashMap<Index<?>, Ilk.Box>();
    
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
        Ilk<IndexSchema<T, F>> indexSchemaIlk = new Ilk<IndexSchema<T,F>>() { }.assign((TypeVariable<?>) new Ilk<T>() {}.key.type, binSchema.getIlk().key.type)
        .assign((TypeVariable<?>) new Ilk<F>() {}.key.type, index.getIlk().key.type);
        Ilk.Box box = table.get(index);
        if (box == null)
        {
            box = indexSchemaIlk.box(new IndexSchema<T, F>(binSchema.getIlk(), index));
            table.put(index, box);
        }
        return box.cast(indexSchemaIlk);
    }
}
