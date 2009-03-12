package com.goodworkalan.memento;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Extractor;

// TODO Document.
public class BinExtractor
implements Extractor<BinRecord, Long>
{
    // TODO Document.
    public Long extract(Stash stash, BinRecord record)
    {
        return record.key;
    }
}

