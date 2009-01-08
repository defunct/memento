package com.goodworkalan.memento;

import java.io.Serializable;

import com.goodworkalan.memento.Join.Index;

public class JoinIndex
final static class Index
implements Serializable
{
    private static final long serialVersionUID = 20070903L;

    public final Object strata;

    public final String[] fields;

    public Index(Strata strata, String[] fields)
    {
        this.strata = strata;
        this.fields = fields;
    }

    private Index(Strata.Schema strata, String[] fields)
    {
        this.strata = strata;
        this.fields = fields;
    }

    public Strata getStrata()
    {
        return (Strata) strata;
    }

    public Join.Index toStrata(Object txn)
    {
        return new Index(((Strata.Schema) strata).newStrata(txn), fields);
    }

    public Join.Index toSchema()
    {
        return new Index(getStrata().getSchema(), fields);
    }
}


