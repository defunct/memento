package com.goodworkalan.memento;

import com.goodworkalan.pack.Pack;

public class JoinJanitor
public final static class Janitor
implements Janitor
{
    private static final long serialVersionUID = 20070826L;

    private final Strata[] isolation;

    private final String name;

    public Janitor(Strata[] isolation, String name)
    {
        this.isolation = isolation;
        this.name = name;
    }

    public void rollback(Snapshot snapshot)
    {
        Join join = snapshot.getJoin(name);
        for (int i = 0; i < join.schema.indices.length; i++)
        {
            Strata.Query query = join.schema.indices[i].getQuery().query(Fossil.txn(join.mutator));
            Strata.Cursor cursor = isolation[i].query(Fossil.txn(join.mutator)).first();
            while (cursor.hasNext())
            {
                query.remove((com.goodworkalan.memento.Record) cursor.next());
            }
            cursor.release();
            query.flush();
        }

    }

    public void dispose(Pack.Mutator mutator, boolean deallocate)
    {
        for (int i = 0; i < isolation.length; i++)
        {
            isolation[i].query(Fossil.txn(mutator)).destroy();
        }
    }
}