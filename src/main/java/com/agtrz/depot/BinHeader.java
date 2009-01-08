package com.agtrz.depot;

import java.io.Serializable;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Tree;

public final class BinHeader
implements Serializable
{
    private static final long serialVersionUID = 20070208L;
    
    private final Tree<BinRecord, Mutator> strata;
    
    private long next;

    public BinHeader(Tree<BinRecord, Mutator> strata)
    {
        this.strata = strata;
    }
    
    public Tree<BinRecord, Mutator> getStrata()
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