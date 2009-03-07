package com.goodworkalan.memento;

import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Strata;

public class JoinStorage
{
    private final Pack pack;
    
    private final Strata<JoinRecord, KeyList> strata;
    
    public JoinStorage(Pack pack, Strata<JoinRecord, KeyList> strata)
    {
        this.pack = pack;
        this.strata = strata;
    }
    
    public Pack getPack()
    {
        return pack;
    }
    
    public Strata<JoinRecord, KeyList> getStrata()
    {
        return strata;
    }
}
