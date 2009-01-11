package com.goodworkalan.memento;


public class JoinIndex
{
    public final JoinSchema joinSchema;

    public final Link alternate;

    public final int[] order;

    public JoinIndex(JoinSchema joinSchema, Link alternate, int[] order)
    {
        this.joinSchema = joinSchema;
        this.alternate = alternate;
        this.order = order;
    }
}


