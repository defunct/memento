package com.goodworkalan.memento;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Extractor;

public class SnapshotExtractor
implements Extractor<SnapshotRecord, Long>
{
    public Long extract(Stash stash, SnapshotRecord object)
    {
        return object.version;
    }
}