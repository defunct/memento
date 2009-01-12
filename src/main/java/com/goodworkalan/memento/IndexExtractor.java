package com.goodworkalan.memento;

import static com.goodworkalan.memento.IndexSchema.EXTRACTOR;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Extractor;

public class IndexExtractor<T, F extends Comparable<F>>
implements Extractor<IndexRecord, F>
{
    private final Item<T> item;
    
    private final Index<F> index;

    public IndexExtractor(Item<T> item, Index<F> index)
    {
        this.item = item;
        this.index = index;
    }
    
    public F extract(Stash stash, IndexRecord object)
    {
        BinTable bins = stash.get(EXTRACTOR, BinTable.class);
        Bin<T> bin = bins.get(item);
        IndexSchema<T, F> indexSchema = bin.getBinSchema().getIndexSchemas().get(index);
        return indexSchema.getIndexer().index(bin.box(object.key, object.version).getItem());
    }
}