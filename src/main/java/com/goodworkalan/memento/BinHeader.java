package com.goodworkalan.memento;

import java.io.Serializable;

import com.goodworkalan.strata.Query;

public final class BinHeader
implements Serializable
{
    private static final long serialVersionUID = 20070208L;
    
    private final Query<BinRecord, Long> strata;
    
    private long next;

    public BinHeader(Query<BinRecord, Long> strata)
    {
        this.strata = strata;
    }
    
    public Query<BinRecord, Long> getStrata()
    {
        return strata;
    }
    
    public long getNext()
    {
        return next;
    }

    public void setNext(long next)
    {
        this.next = next;
    }
}