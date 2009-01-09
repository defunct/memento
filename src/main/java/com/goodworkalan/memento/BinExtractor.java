package com.goodworkalan.memento;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Extractor;

public class BinExtractor
implements Extractor<BinRecord, Long, Mutator>
{
    public Long extract(Mutator txn, BinRecord record)
    {
        return record.key;
    }
}

