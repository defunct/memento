package com.goodworkalan.memento;

public class IndexBuilder<T, F>
{
    private final BinBuilder<T> binBuilder;
    
    private final Item<T> item;
    
    private final Index<F> index;
    
    private boolean unique;
    
    private Indexer<T, F> indexer; 
    
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
        binSchemas.get(item).getIndexTable().get(index).setIndexer(indexer);
        // TODO Maybe reindex.
        this.indexer = indexer;
        return this;
    }
    
    public IndexBuilder<T, F> unique(boolean unique)
    {
        this.unique = unique;
        return this;
    }
    
    public BinBuilder<T> end()
    {
        return binBuilder;
    }
}
