package com.goodworkalan.memento;

public class IdGenerator
{
    private long identifier;
    
    public IdGenerator(long identifier)
    {
        this.identifier = identifier;
    }
    
    public synchronized long next()
    {
        return ++identifier;
    }
}
