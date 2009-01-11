package com.goodworkalan.memento;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Query;

public class StrataMutator<T, F extends Comparable<F>>
{
    private final Query<T, F> query;
    
    private final Mutator mutator;
    
    public StrataMutator(Query<T, F> query, Mutator mutator)
    {
        this.query = query;
        this.mutator = mutator;
    }
    
    public Query<T, F> getQuery()
    {
        return query;
    }
    
    public Mutator getMutator()
    {
        return mutator;
    }
}
