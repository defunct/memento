package com.goodworkalan.memento;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;

public class JoinBuilder
{
    private final JoinIndex joinIndex;
    
    private final List<Object> values;
    
    private final List<Ilk.Key> items;
    
    public JoinBuilder(JoinIndex joinIndex)
    {
        this.joinIndex = joinIndex;
        this.values = new ArrayList<Object>();
        this.items = joinIndex.link.getIlkKeys();
    }
    
    public Map<Ilk.Key, List<Object>> duplicate(Map<Ilk.Key, Integer> prototype)
    {
        Map<Ilk.Key, List<Object>> join = new HashMap<Ilk.Key, List<Object>>();
        for (Ilk.Key key : prototype.keySet())
        {
            join.put(key, new ArrayList<Object>(prototype.get(key)));
        }
        return join;
    }
    
    public <T> JoinBuilder push(Ilk<T> ilk, T value)
    {
        push(ilk.key, value);
        return this;
    }
    
    private <T> void push(Ilk.Key key, T value)
    {
        if (values.size() == items.size())
        {
            throw new IllegalStateException();
        }
        if (!items.get(values.size()).equals(key))
        {
            throw new IllegalStateException();
        }
        values.add(value);
    }
    
    public <T> JoinBuilder push(Class<T> itemClass, T value)
    {
        push(new Ilk.Key(itemClass), value);
        return this;
    }
    
    public void add()
    {
        joinIndex.getClass();
    }
}
