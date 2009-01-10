package com.goodworkalan.memento;

public class JoinAdd<T>
{
    private final JoinBuilder joinBuilder;
    
    private final Item<T> item;
    
    public JoinAdd(JoinBuilder joinBuilder, Item<T> item)
    {
        this.joinBuilder = joinBuilder;
        this.item = item;
    }
    
    public void add(T value)
    {
        joinBuilder.set(item, value);
    }
}
