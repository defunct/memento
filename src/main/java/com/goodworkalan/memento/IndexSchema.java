package com.goodworkalan.memento;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Strata;

public class IndexSchema<T, F extends Comparable<? super F>>
{
    final static Stash.Key EXTRACTOR = new Stash.Key();
    
    private Indexer<T, F> indexer;

    private Strata<IndexRecord> strata;

    private boolean unique;

    private boolean notNull;

    private final Ilk<T> ilk;
    
    private final Index<F> index;

    public IndexSchema(Ilk<T> ilk, Index<F> index)
    {
        this.ilk = ilk;
        this.index = index;
    }

    public Ilk<T> getIlk()
    {
        return ilk;
    }
    
    public Index<F> getIndex()
    {
        return index;
    }
    
    public Strata<IndexRecord> getStrata()
    {
        return strata;
    }
    
    public void setIndexer(Indexer<T, F> indexer)
    {
        this.indexer = indexer;
    }
    
    public Indexer<T, F> getIndexer()
    {
        return indexer;
    }
    
    public void setUnique(boolean unique)
    {
        this.unique = unique;
    }
    
    public boolean isUnique()
    {
        return unique;
    }
    
    public void setNotNull(boolean notNull)
    {
        this.notNull = notNull;
    }
    
    public boolean isNotNull()
    {
        return notNull;
    }
}
