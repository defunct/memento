package com.goodworkalan.memento;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Extractor;

public class BinExtractor
implements Extractor<BinRecord, Long>
{
    public Long extract(Stash stash, BinRecord record)
    {
        return record.key;
    }
}

