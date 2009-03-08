package com.goodworkalan.memento;

import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Strata;

public class IndexStorage
{
    private final Pack pack;
    
    private final Strata<IndexRecord> strata;
    
    public IndexStorage(Pack pack, Strata<IndexRecord> strata)
    {
        this.pack = pack;
        this.strata = strata;
    }
    
    public Pack getPack()
    {
        return pack;
    }
    
    public Strata<IndexRecord> getStrata()
    {
        return strata;
    }
}
