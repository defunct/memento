package com.goodworkalan.memento;

public class JoinBuilder
{
    public <T> JoinBinBuilder<T, End> bin(Class<T> itemClass)
    {
        return new JoinBinBuilder<T, End>();
    }
}
