package com.goodworkalan.memento;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Extractor;


public class JoinExtractor
implements Extractor<JoinRecord, Ordered>
{
    public Ordered extract(Stash stash, JoinRecord object)
    {
        Long[] copy = new Long[object.keys.length];
        for (int i = 0; i < copy.length; i++)
        {
            copy[i] = object.keys[i];
        }
        return new Ordered(copy);
    }
}