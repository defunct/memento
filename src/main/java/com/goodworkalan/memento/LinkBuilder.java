package com.goodworkalan.memento;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LinkBuilder
{
    private final Link link;
    
    private final Store store; 
    
    private final List<Integer[]> alternates;
    
    public LinkBuilder(Store store, Link link)
    {
        this.store = store;
        this.link = link;
        this.alternates = new ArrayList<Integer[]>();
    }

    public void alternate(Integer...fields)
    {
        List<Item<?>> items = link.getItems();
        Set<Integer> seen = new HashSet<Integer>();
        if (fields.length > items.size())
        {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < fields.length; i++)
        {
            if (seen.contains(fields[i]))
            {
                throw new IllegalArgumentException();
            }
            if (fields[i] >= items.size())
            {
                throw new IllegalArgumentException();
            }
            seen.add(fields[i]);
        }
        alternates.add(fields);
    }
    
    public Store end()
    {
        return store;
    }
}
