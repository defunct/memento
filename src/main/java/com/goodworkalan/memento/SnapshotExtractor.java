package com.goodworkalan.memento;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Extractor;

// TODO Document.
public class SnapshotExtractor
implements Extractor<SnapshotRecord, Long>
{
    // TODO Document.
    public Long extract(Stash stash, SnapshotRecord object)
    {
        return object.version;
    }
}