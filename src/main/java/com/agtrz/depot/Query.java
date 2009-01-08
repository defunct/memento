package com.agtrz.depot;

import java.util.ArrayList;
import java.util.List;


public final class Query<T>
extends TypeReference<T>
{
    private final Snapshot snapshot;
    
    private final List<Criteria> and;
    
    public Query(Snapshot snapshot)
    {
        this.snapshot = snapshot;
        this.and = new ArrayList<Criteria>();
    }
    
    public void equalTo(String property,Object value)
    {
        and.add(new Equals(value));
    }
    
    public T getSingleObject()
    {
        snapshot.getBin(getType().toString());
        return null;
    }
}