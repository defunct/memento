package com.goodworkalan.memento;

public class Revision
{
    private final long key;
    
    private final long version;
    
    public Revision(long key, long version)
    {
        this.key = key;
        this.version = version;
    }
    
    public long getKey()
    {
        return key;
    }
    
    public long getVersion()
    {
        return version;
    }
}
