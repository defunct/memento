package com.goodworkalan.memento;


public class JoinIndex
{
    public final JoinSchema joinSchema;

    public final Link link;

    public final int[] order;

    public JoinIndex(JoinSchema joinSchema, Link alternate, int[] order)
    {
        this.joinSchema = joinSchema;
        this.link = alternate;
        this.order = order;
    }
    
    public Link getLink()
    {
        return link;
    }
}


