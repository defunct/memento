package com.goodworkalan.memento;

import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Strata;

public class IndexStorage<F extends Comparable<F>>
{
    private final Pack pack;
    
    private final Strata<IndexRecord, F> strata;
    
    public IndexStorage(Pack pack, Strata<IndexRecord, F> strata)
    {
        this.pack = pack;
        this.strata = strata;
    }
    
    public Pack getPack()
    {
        return pack;
    }
    
    public Strata<IndexRecord, F> getStrata()
    {
        return strata;
    }
}
