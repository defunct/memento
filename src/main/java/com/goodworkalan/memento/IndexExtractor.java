package com.goodworkalan.memento;

import static com.goodworkalan.memento.IndexSchema.EXTRACTOR;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Extractor;

public class IndexExtractor<T, F>
implements Extractor<IndexRecord, Ordered>
{
    private final TypeKey<Indexer<T, F>> key;
    
    private final Indexer<Item, Fields> indexer;
    
    public Ordered extract(Stash stash, IndexRecord object)
    {
        TypeStash typeStash = stash.get(EXTRACTOR, TypeStash.class);
        
        IndexSchema<Item, Fields> schema = stash.get(EXTRACTOR, new Stash.Type<IndexSchema<Item, Fields>>() { });
        return new Ordered(object.key, object.version);
    }
}