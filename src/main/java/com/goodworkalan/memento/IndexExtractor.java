package com.goodworkalan.memento;

import java.io.Serializable;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Extractor;
import com.goodworkalan.strata.Record;

public class IndexExtractor
implements Extractor<IndexRecord, Mutator>, Serializable
{
    private static final long serialVersionUID = 20070403L;

    public void extract(Mutator txn, IndexRecord object, Record record)
    {
        record.fields(object.key, object.version);
    }
}