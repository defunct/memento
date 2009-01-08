package com.goodworkalan.memento;

public class Box<Item>
{
    private final long key;
    
    private final long version;
    
    private final Item item;
    
    public Box(long key, long version, Item item)
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
    
    public Item getItem()
    {
        return item;
    }
}
