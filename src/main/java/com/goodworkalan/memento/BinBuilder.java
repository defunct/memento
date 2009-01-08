package com.goodworkalan.memento;

public class BinBuilder<Item>
{
    private final Class<Item> itemClass;
    
    private ItemIO<Item> io;
    
    public BinBuilder(Class<Item> itemClass)
    {
        this.itemClass = itemClass;
        this.io = SerializationIO.getInstance(itemClass);
    }
    
    public <Child extends Item> BinBuilder<Item> subclass(Class<Child> subclass)
    {
        return this;
    }
    
    public BinBuilder<Item> io(ItemIO<Item> io)
    {
        this.io = io;
        return this;
    }
    
    public <Fields> BinBuilder<Item> index(String name, Indexer<Item, Fields> indexer)
    {
        return this;
    }
}
