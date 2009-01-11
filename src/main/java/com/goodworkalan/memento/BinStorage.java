package com.goodworkalan.memento;

import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Strata;

public class BinStorage
{
    public final Pack pack;
    
    public final Strata<BinRecord, Long> strata;
    
    public BinStorage(Pack pack, Strata<BinRecord, Long> strata)
    {
        this.pack = pack;
        this.strata = strata;
    }
}
