package com.goodworkalan.memento;

public class IndexBuilder<T, F extends Comparable<? super F>>
{
    private final BinBuilder<T> binBuilder;
    
    private final Item<T> item;
    
    private final Index<F> index;
    
    private final BinSchemaTable binSchemas;
    
    public IndexBuilder(BinBuilder<T> binBuilder, BinSchemaTable binSchemas, Item<T> item, Index<F> index)
    {
        this.binBuilder = binBuilder;
        this.binSchemas = binSchemas;
        this.item = item;
        this.index = index;
    }
    
    public IndexBuilder<T, F> indexer(Indexer<T, F> indexer)
    {
        binSchemas.get(item).getIndexSchemas().get(index).setIndexer(indexer);
        return this;
    }
    
    public IndexBuilder<T, F> unique(boolean unique)
    {
        binSchemas.get(item).getIndexSchemas().get(index).setUnique(unique);
        return this;
    }
    
    public IndexBuilder<T, F> notNull(boolean notNull)
    {
        binSchemas.get(item).getIndexSchemas().get(index).setNotNull(notNull);
        return this;
    }
    
    public BinBuilder<T> end()
    {
        return binBuilder;
    }
}
