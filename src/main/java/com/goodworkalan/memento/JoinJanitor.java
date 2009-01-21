package com.goodworkalan.memento;

import java.util.List;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Strata;

public class JoinJanitor
implements Janitor
{
    private static final long serialVersionUID = 1L;

    private final List<Strata<JoinRecord, Ordered>> isolations;

    private final String name;

    public JoinJanitor(List<Strata<JoinRecord, Ordered>> isolation, String name)
    {
        this.isolations = isolation;
        this.name = name;
    }

    public void rollback(Snapshot snapshot)
    {
        Join join = snapshot.getJoin(name);
        for (int i = 0; i < join.schema.indices.length; i++)
        {
            Strata.Query query = join.schema.indices[i].getQuery().query(Fossil.txn(join.mutator));
            Strata.Cursor cursor = isolations[i].query(Fossil.txn(join.mutator)).first();
            while (cursor.hasNext())
            {
                query.remove(cursor.next());
            }
            cursor.release();
            query.flush();
        }
    }

    public void dispose(Mutator mutator, boolean deallocate)
    {
        for (Strata<JoinRecord, Ordered> isolation : isolations)
        {
            isolation.query(Fossil.initialize(new Stash(), mutator)).destroy();
        }
    }
}