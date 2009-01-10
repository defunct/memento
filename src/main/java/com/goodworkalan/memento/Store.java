package com.goodworkalan.memento;


public class Store
{
    private final BinSchemaTable binSchemas = new BinSchemaTable();
    
    public <T> BinBuilder<T> store(Class<T> itemClass)
    {
        return new BinBuilder<T>(this, binSchemas, new Item<T>(itemClass) {});
    }
    
    public <T> BinBuilder<T> store(Item<T> item)
    {
        return new BinBuilder<T>(this, binSchemas, item);
    }
    
    public LinkBuilder link(Link link)
    {
        return null;
    }

    public Snapshot newSnapshot(Sync sync)
    {
        return null;
    }
    
    public Snapshot newSnapshot()
    {
        return null;
    }
    
    public <From, To> OneToMany<From, To> toMany(From from, Class<To> to)
    {
        return new OneToMany<From, To>();
    }
    
    public <From, To> OneToMany<From, To> toMany(From from, Class<To> to, String name)
    {
        return new OneToMany<From, To>();
    }
}
