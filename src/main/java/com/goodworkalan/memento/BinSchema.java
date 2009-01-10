package com.goodworkalan.memento;

import java.io.Serializable;

import com.goodworkalan.strata.Strata;

final class BinSchema<T>
implements Serializable
{
    private static final long serialVersionUID = 20070408L;

    private final IndexTable indexTable;
    
    private Strata<BinRecord, Long> strata;

    private ItemIO<T> io;
    
    private long identifier;

    public BinSchema()
    {
        this.indexTable = new IndexTable();
    }

    public Strata<BinRecord, Long> getStrata()
    {
        return strata;
    }
    
    public void setItemIO(ItemIO<T> io)
    {
        // TODO Reindex if changed.
        this.io = io;
    }
    
    public ItemIO<T> getItemIO()
    {
        return io;
    }
    
    public IndexTable getIndexTable()
    {
        return indexTable;
    }
    
    public long nextIdentifier()
    {
        return ++identifier;
    }
    
    public void setIdentifierIf(long identifier)
    {
        if (this.identifier < identifier)
        {
            this.identifier = identifier;
        }
    }
}