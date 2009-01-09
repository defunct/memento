package com.goodworkalan.memento;

import com.goodworkalan.strata.Extractor;

public class IndexExtractor<X>
implements Extractor<IndexRecord, Ordered, X>
{
    public Ordered extract(X txn, IndexRecord object)
    {
        return new Ordered(object.key, object.version);
    }
}