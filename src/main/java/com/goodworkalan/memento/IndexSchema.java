package com.goodworkalan.memento;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Strata;

public class IndexSchema<T, F>
{
    final static Stash.Key EXTRACTOR = new Stash.Key();
    
    private Indexer<T, F> indexer;

    private Strata<IndexRecord, Ordered> strata;

    private boolean unique;

    private boolean notNull;

    private ItemIO<T> io;
    
    private final Item<T> item;
    
    private final Index<F> index;

    public IndexSchema(Item<T> item, Index<F> index)
    {
        this.item = item;
        this.index = index;
    }

    public Item<T> getItem()
    {
        return item;
    }
    
    public Index<F> getIndex()
    {
        return index;
    }
    
    public Strata<IndexRecord, Ordered> getStrata()
    {
        return strata;
    }
    
    public void setItemIO(ItemIO<T> io)
    {
        this.io = io;
    }
    
    public ItemIO<T> getItemIO()
    {
        return io;
    }
    
    public void setIndexer(Indexer<T, F> indexer)
    {
        this.indexer = indexer;
    }
    
    public Indexer<T, F> getIndexer()
    {
        return indexer;
    }
}
