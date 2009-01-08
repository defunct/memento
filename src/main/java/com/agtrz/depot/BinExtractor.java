package com.agtrz.depot;

import java.io.Serializable;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Extractor;
import com.goodworkalan.strata.Record;

public class BinExtractor
implements Extractor<BinRecord, Mutator>, Serializable
{
    private static final long serialVersionUID = 20070408L;

    public void extract(Mutator txn, BinRecord object, Record record)
    {
        record.fields(object.key);
    }
}

