package com.goodworkalan.memento;

class BinCommon
{
    long identifier;

    public BinCommon(long identifier)
    {
        this.identifier = identifier;
    }

    public synchronized Long nextIdentifier()
    {
        return new Long(identifier++);
    }
}
