package com.goodworkalan.memento;

import java.io.Serializable;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Extractor;
import com.goodworkalan.strata.Record;


public class JoinExtractor
implements Extractor<JoinRecord, Mutator>, Serializable
{
    private static final long serialVersionUID = 20070403L;

    public void extract(Mutator txn, JoinRecord object, Record record)
    {
        Long[] copy = new Long[object.keys.length];
        for (int i = 0; i < copy.length; i++)
        {
            copy[i] = object.keys[i];
        }
        record.fields(copy);
    }
}