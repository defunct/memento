package com.goodworkalan.memento;

import java.io.Serializable;

import com.goodworkalan.strata.Query;

// TODO Document.
public final class BinHeader
implements Serializable
{
    // TODO Document.
    private static final long serialVersionUID = 20070208L;
    
    // TODO Document.
    private final Query<BinRecord> strata;
    
    // TODO Document.
    private long next;

    // TODO Document.
    public BinHeader(Query<BinRecord> strata)
    {
        this.strata = strata;
    }
    
    // TODO Document.
    public Query<BinRecord> getStrata()
    {
        return strata;
    }
    
    // TODO Document.
    public long getNext()
    {
        return next;
    }

    // TODO Document.
    public void setNext(long next)
    {
        this.next = next;
    }
}