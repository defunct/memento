package com.goodworkalan.memento;

public class Box<T>
{
    private final long key;
    
    private final long version;
    
    private final T item;
    
    public Box(long key, long version, T item)
    {
        this.key = key;
        this.version = version;
        this.item = item;
    }
    
    public long getKey()
    {
        return key;
    }
    
    public long getVersion()
    {
        return version;
    }
    
    public T getItem()
    {
        return item;
    }
}
