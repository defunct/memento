package com.goodworkalan.memento;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinBuilder
{
    private final Map<Item<?>, List<Object>> join;
    
    public JoinBuilder(Map<Item<?>, Integer> prototype)
    {
        this.join = duplicate(prototype);
    }
    
    public Map<Item<?>, List<Object>> duplicate(Map<Item<?>, Integer> prototype)
    {
        Map<Item<?>, List<Object>> join = new HashMap<Item<?>, List<Object>>();
        for (Item<?> key : prototype.keySet())
        {
            join.put(key, new ArrayList<Object>(prototype.get(key)));
        }
        return join;
    }
    
    public <T> JoinBuilder set(int index, Item<T> item, T value)
    {
        List<Object> values = join.get(item);
        if (join == null)
        {
            values = new ArrayList<Object>();
            values.add(value);
        }
        join.put(item, values);
        return this;
    }
    
    public <T> JoinBuilder set(int index, Class<T> itemClass, T value)
    {
        return set(index, new Item<T>(itemClass) {}, value);
    }
    
    public <T> JoinBuilder set(Class<T> item, T value)
    {
        return set(0, item, value);
    }
    
    public <T> JoinBuilder set(Item<T> item, T value)
    {
        return set(0, item, value);
    }
    
    public void add()
    {
    }
}
