package com.goodworkalan.memento;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Construction;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;

public class JoinJanitor
implements Janitor
{
    private static final long serialVersionUID = 1L;

    private final Storage storage;

    private final Link link;
    
    private final Construction<JoinRecord, KeyList, Long> isolation;

    public JoinJanitor(Storage storage, Link link, Construction<JoinRecord, KeyList, Long> isolation)
    {
        this.storage = storage;
        this.link = link;
        this.isolation = isolation;
    }

    public void rollback(Snapshot snapshot)
    {
        // FIXME Who gives me a mutator?
        Query<JoinRecord, KeyList> common = storage.open(link).getStrata().query(Fossil.initialize(new Stash(), null));
        
        Cursor<JoinRecord> cursor = isolation.getQuery().first();
        while (cursor.hasNext())
        {
            common.remove(common.extract(cursor.next()));
        }
        cursor.release();
        isolation.getQuery().flush();
    }

    public void dispose(Mutator mutator, boolean deallocate)
    {
        isolation.getQuery().destroy();
    }
}