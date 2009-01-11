package com.goodworkalan.memento;

import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Strata;

public class JoinStorage
{
    private final Pack pack;
    
    private final Strata<JoinRecord, Ordered> strata;
    
    public JoinStorage(Pack pack, Strata<JoinRecord, Ordered> strata)
    {
        this.pack = pack;
        this.strata = strata;
    }
    
    public Pack getPack()
    {
        return pack;
    }
    
    public Strata<JoinRecord, Ordered> getStrata()
    {
        return strata;
    }
}
