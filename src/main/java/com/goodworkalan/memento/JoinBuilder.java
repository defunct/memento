package com.goodworkalan.memento;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinBuilder
{
    private final JoinIndex joinIndex;
    
    private final List<Object> values;
    
    private final List<Item<?>> items;
    
    public JoinBuilder(JoinIndex joinIndex)
    {
        this.joinIndex = joinIndex;
        this.values = new ArrayList<Object>();
        this.items = joinIndex.alternate.getItems();
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
    
    public <T> JoinBuilder push(Item<T> item, T value)
    {
        if (values.size() == items.size())
        {
            throw new IllegalStateException();
        }
        if (!items.get(values.size()).equals(item))
        {
            throw new IllegalStateException();
        }
        values.add(value);
        return this;
    }
    
    public <T> JoinBuilder push(Class<T> itemClass, T value)
    {
        return push(new Item<T>(itemClass) {}, value);
    }
    
    public void add()
    {
        joinIndex.getClass();
    }
}
