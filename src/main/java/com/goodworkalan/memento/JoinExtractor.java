package com.goodworkalan.memento;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Extractor;


public class JoinExtractor
implements Extractor<JoinRecord, KeyList>
{
    public KeyList extract(Stash stash, JoinRecord joinRecord)
    {
        KeyList keys = new KeyList(joinRecord.keys.length);
        for (int i = 0; i < joinRecord.keys.length; i++)
        {
            keys.add(joinRecord.keys[i]);
        }
        return keys;
    }
}