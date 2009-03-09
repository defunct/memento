package com.goodworkalan.memento;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Strata;

public class JoinJanitor
implements Janitor
{
    private static final long serialVersionUID = 1L;

    private final Storage storage;

    private final Link link;
    
    private final Strata<JoinRecord> isolation;

    public JoinJanitor(Storage storage, Link link, Strata<JoinRecord> isolation)
    {
        this.storage = storage;
        this.link = link;
        this.isolation = isolation;
    }

    public void rollback(Snapshot snapshot)
    {
        // FIXME Who gives me a mutator?
        Query<JoinRecord> common = storage.open(link).getStrata().query(Fossil.initialize(new Stash(), null));
        
        Cursor<JoinRecord> cursor = isolation.query().first();
        while (cursor.hasNext())
        {
            common.remove(common.comparable(cursor.next()));
        }
        cursor.release();
    }

    public void dispose(Mutator mutator, boolean deallocate)
    {
        isolation.query(Fossil.newStash(mutator)).destroy();
    }
}