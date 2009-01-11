package com.goodworkalan.memento;

public class LinkAlternate
{
    private final int[] order;
    
    private final Link alternate;
    
    public LinkAlternate(Link alternate, int[] order)
    {
        this.alternate = alternate;
        this.order = order;
    }
    
    public Link getAlternate()
    {
        return alternate;
    }
    
    public int[] getOrder()
    {
        return order;
    }
}
