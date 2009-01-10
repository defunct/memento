package com.goodworkalan.memento;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Strata;

public class IndexSchema<T, F>
{
    final static Stash.Key EXTRACTOR = new Stash.Key();
    
    private Indexer<T, F> extractor;

    private Strata<IndexRecord, Ordered> strata;

    private boolean unique;

    private boolean notNull;

    private ItemIO<T> io;

    public IndexSchema()
    {
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
}
