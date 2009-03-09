package com.goodworkalan.memento;

import com.goodworkalan.ilk.Ilk;

public class JoinAdd<T>
{
    private final JoinBuilder joinBuilder;
    
    private final Ilk<T> ilk;
    
    public JoinAdd(JoinBuilder joinBuilder, Ilk<T> item)
    {
        this.joinBuilder = joinBuilder;
        this.ilk = item;
    }
    
    public void add(T value)
    {
        joinBuilder.push(ilk, value);
    }
}
