package com.goodworkalan.memento;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LinkBuilder
{
//    private final JoinSchemaTable joinSchemas;
    
    private final Link link;
    
    private final Store store; 
    
    private final List<LinkAlternate> alternates;
    
    public LinkBuilder(JoinSchemaTable joinSchemas, Store store, Link link)
    {
//        this.joinSchemas = joinSchemas;
        this.store = store;
        this.link = link;
        this.alternates = newAlternates(); 
    }
    
    public List<LinkAlternate> newAlternates()
    {
        List<LinkAlternate> alternates = new ArrayList<LinkAlternate>();
        int[] order = new int[link.size()];
        for (int i = 0; i < link.size(); i++) 
        {
            order[i] = i;
        }
        alternates.add(new LinkAlternate(link, order));
        return alternates;
    }
    
    public LinkBuilder biDirectional(Link alternate)
    {
        if (alternate.size() != 2)
        {
            throw new IllegalArgumentException();
        }
        List<Item<?>> these = link.getItems();
        List<Item<?>> those = alternate.getItems();
        if (!these.get(0).equals(those.get(1)) || !these.get(1).equals(those.get(0)))
        {
            throw new IllegalArgumentException();
        }
        return alternate(alternate, 1, 0);
    }

    public LinkBuilder alternate(Link alternate, int...fields)
    {
        List<Item<?>> these = link.getItems();
        List<Item<?>> those = link.getItems();
        if (these.size() != those.size())
        {
            throw new IllegalArgumentException();
        }
        if (fields.length == 0)
        {
            Map<Item<?>, int[]> seen = new HashMap<Item<?>, int[]>(); 
            fields = new int[these.size()];
            for (int i = 0; i <  those.size(); i++)
            {
                Item<?> item = those.get(i);
                if (!seen.containsKey(item))
                {
                    seen.put(item, new int[] { 0 });
                }
                int found = -1;
                int count = seen.get(item)[0] + 1;
                for (int j = 0; j < count; j++)
                {
                    found = -1;
                    for (int k = 0; found == -1 && k < these.size(); k++)
                    {
                        if (these.get(k).equals(item))
                        {
                            found = k;
                        }
                    }
                }
                if (found == -1)
                {
                    throw new IllegalArgumentException();
                }
                fields[i] = found;
            }
        }
        else if (fields.length == these.size())
        {
            for (int i = 0; i < fields.length; i++)
            {
                if (fields[i] >= these.size())
                {
                    throw new IllegalArgumentException();
                }
            }
        }
        else
        {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < those.size(); i++)
        {
            if (!these.get(fields[i]).equals(those.get(i)))
            {
                throw new IllegalArgumentException();
            }
        }
        alternates.add(new LinkAlternate(alternate, fields));
        return this;
    }
    
    public Store end()
    {
        return store;
    }
}
