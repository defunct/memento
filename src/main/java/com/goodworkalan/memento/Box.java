package com.goodworkalan.memento;

// TODO Document.
public class Box<T>
{
    // TODO Document.
    private final long key;
    
    // TODO Document.
    private final long version;
    
    // TODO Document.
    private final T item;
    
    // TODO Document.
    public Box(long key, long version, T item)
    {
        this.key = key;
        this.version = version;
        this.item = item;
    }
    
    // TODO Document.
    public long getKey()
    {
        return key;
    }
    
    // TODO Document.
    public long getVersion()
    {
        return version;
    }
    
    // TODO Document.
    public T getItem()
    {
        return item;
    }
}
