package com.goodworkalan.memento;

import java.io.Serializable;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.strata.Strata;

final class BinSchema<T>
implements Serializable
{
    private static final long serialVersionUID = 20070408L;

    private final IndexSchemaTable<T> indexSchemas;
    
    private Strata<BinRecord> strata;

    private final Ilk<T> ilk;

    private ItemIO<T> io;
    
    private long identifier;
    
    public BinSchema(Ilk<T> ilk)
    {
        this.ilk = ilk;
        this.indexSchemas = new IndexSchemaTable<T>(this);
    }

    public Ilk<T> getIlk()
    {
        return ilk;
    }

    public Strata<BinRecord> getStrata()
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
    
    public IndexSchemaTable<T> getIndexSchemas()
    {
        return indexSchemas;
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