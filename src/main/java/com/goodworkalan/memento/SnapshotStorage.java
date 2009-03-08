package com.goodworkalan.memento;

import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Strata;

public class SnapshotStorage
{
    private final Strata<SnapshotRecord> strata;
    
    private final Pack pack;
    
    public SnapshotStorage(Pack pack, Strata<SnapshotRecord> strata)
    {
        this.strata = strata;
        this.pack = pack;
    }
    
    public Strata<SnapshotRecord> getStrata()
    {
        return strata;
    }
    
    public Pack getPack()
    {
        return pack;
    }
}
