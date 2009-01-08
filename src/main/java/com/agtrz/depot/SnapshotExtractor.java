package com.agtrz.depot;

import java.io.Serializable;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Extractor;
import com.goodworkalan.strata.Record;

public class SnapshotExtractor
implements Extractor<SnapshotRecord, Mutator>, Serializable
{
    private static final long serialVersionUID = 20070409L;

    public void extract(Mutator txn, SnapshotRecord object, Record record)
    {
        record.fields(object.version);
    }
}