package com.goodworkalan.memento;

// TODO Document.
class BinCommon
{
    // TODO Document.
    long identifier;

    // TODO Document.
    public BinCommon(long identifier)
    {
        this.identifier = identifier;
    }

    // TODO Document.
    public synchronized Long nextIdentifier()
    {
        return new Long(identifier++);
    }
}
