package com.goodworkalan.memento;

import com.goodworkalan.strata.Strata;

public class IndexSchema<Item, Fields>
{
    public final Indexer<Item, Fields> extractor;

    public final Strata<IndexRecord, Ordered> strata;

    public final boolean unique;

    public final boolean notNull;

    public final ItemIO<Item> io;

    public IndexSchema(Strata<IndexRecord, Ordered> strata, Indexer<Item, Fields> extractor, boolean unique, boolean notNull, ItemIO<Item> io)
    {
        this.extractor = extractor;
        this.strata = strata;
        this.unique = unique;
        this.notNull = notNull;
        this.io = io;
    }

    public Strata<IndexRecord, Ordered> getStrata()
    {
        return strata;
    }
}
