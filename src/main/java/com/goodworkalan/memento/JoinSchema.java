package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class JoinSchema implements Iterable<JoinIndex>
{
    private final Link link;
    
    private final Map<Link, JoinIndex> indexes = new HashMap<Link, JoinIndex>();

    public JoinSchema(Link link)
    {
        this.link = link;
    }
    
    public Link getLink()
    {
        return link;
    }
    
    public void add(Link alternate, int[] order)
    {
        JoinIndex index = new JoinIndex(this, alternate, order);
        indexes.put(alternate, index);
    }
    
    public Iterator<JoinIndex> iterator()
    {
        return indexes.values().iterator();
    }
}



