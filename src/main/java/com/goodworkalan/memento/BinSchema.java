package com.goodworkalan.memento;

import java.io.Serializable;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.strata.Strata;

// TODO Document.
final class BinSchema<T>
implements Serializable
{
    // TODO Document.
    private static final long serialVersionUID = 20070408L;

    // TODO Document.
    private final IndexSchemaTable<T> indexSchemas;
    
    // TODO Document.
    private Strata<BinRecord> strata;

    // TODO Document.
    private final Ilk<T> ilk;

    // TODO Document.
    private ItemIO<T> io;
    
    // TODO Document.
    private long identifier;
    
    // TODO Document.
    public BinSchema(Ilk<T> ilk)
    {
        this.ilk = ilk;
        this.indexSchemas = new IndexSchemaTable<T>(this);
    }

    // TODO Document.
    public Ilk<T> getIlk()
    {
        return ilk;
    }

    // TODO Document.
    public Strata<BinRecord> getStrata()
    {
        return strata;
    }
    
    // TODO Document.
    public void setItemIO(ItemIO<T> io)
    {
        // TODO Reindex if changed.
        this.io = io;
    }
    
    // TODO Document.
    public ItemIO<T> getItemIO()
    {
        return io;
    }
    
    // TODO Document.
    public IndexSchemaTable<T> getIndexSchemas()
    {
        return indexSchemas;
    }
    
    // TODO Document.
    public long nextIdentifier()
    {
        return ++identifier;
    }
    
    // TODO Document.
    public void setIdentifierIf(long identifier)
    {
        if (this.identifier < identifier)
        {
            this.identifier = identifier;
        }
    }
}