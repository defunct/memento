package com.goodworkalan.memento;

public class BinCommon
{
    private long identifier;

    public BinCommon(long identifier)
    {
        this.identifier = identifier;
    }

    public synchronized Long nextIdentifier()
    {
        return new Long(identifier++);
    }
}