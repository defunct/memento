package com.goodworkalan.memento;

import static com.goodworkalan.memento.IndexSchema.EXTRACTOR;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Extractor;

public class IndexExtractor<T, F extends Comparable<F>>
implements Extractor<IndexRecord, F>
{
    private final Ilk<T> ilk;
    
    private final Index<F> index;

    public IndexExtractor(Ilk<T> ilk, Index<F> index)
    {
        this.ilk = ilk;
        this.index = index;
    }
    
    public F extract(Stash stash, IndexRecord object)
    {
        BinTable bins = stash.get(EXTRACTOR, BinTable.class);
        Bin<T> bin = bins.get(ilk);
        IndexSchema<T, F> indexSchema = bin.getBinSchema().getIndexSchemas().get(index);
        return indexSchema.getIndexer().index(bin.box(object.key, object.version).getItem());
    }
}