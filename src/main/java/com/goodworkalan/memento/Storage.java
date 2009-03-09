package com.goodworkalan.memento;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Strata;


public interface Storage
{
    public final static Stash.Key MUTATOR = new Stash.Key();
    
    public void open();

    public <T> BinStorage open(Ilk<T> item);

    public <T, F extends Comparable<F>> IndexStorage open(Ilk<T> item, Index<F> index);
    
    public JoinStorage open(Link link);
    
    public Query<SnapshotRecord> newSnapshotQuery();

    public Strata<SnapshotRecord> getSnapshots();
}
