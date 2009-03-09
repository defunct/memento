package com.goodworkalan.memento;

import com.goodworkalan.ilk.Ilk;

public class IndexBuilder<T, F extends Comparable<? super F>>
{
    private final BinBuilder<T> binBuilder;
    
    private final Ilk<T> ilk;
    
    private final Index<F> index;
    
    private final BinSchemaTable binSchemas;
    
    public IndexBuilder(BinBuilder<T> binBuilder, BinSchemaTable binSchemas, Ilk<T> ilk, Index<F> index)
    {
        this.binBuilder = binBuilder;
        this.binSchemas = binSchemas;
        this.ilk = ilk;
        this.index = index;
    }
    
    public IndexBuilder<T, F> indexer(Indexer<T, F> indexer)
    {
        binSchemas.get(ilk).getIndexSchemas().get(index).setIndexer(indexer);
        return this;
    }
    
    public IndexBuilder<T, F> unique(boolean unique)
    {
        binSchemas.get(ilk).getIndexSchemas().get(index).setUnique(unique);
        return this;
    }
    
    public IndexBuilder<T, F> notNull(boolean notNull)
    {
        binSchemas.get(ilk).getIndexSchemas().get(index).setNotNull(notNull);
        return this;
    }
    
    public BinBuilder<T> end()
    {
        return binBuilder;
    }
}
