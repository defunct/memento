package com.goodworkalan.memento;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Store
{
    public <Child, Parent> void corral(Class<Child> superClass, Class<Parent> subClass)
    {
    }
    
    public <Item> long add(Item item)
    {
        return 0L;
    }
    
    public <Item> long getId(Item item)
    {
        return 0L;
    }
    
    public <Item> Item get(Class<Item> itemClass, long id)
    {
        return null;
    }

    public <Item> void update(Item item)
    {
    }

    public <Item> void replace(Item from, Item to)
    {
    }
    
    public <Item> Collection<Item> getAll(Class<Item> itemClass)
    {
        return Collections.emptyList();
    }
    
    public <From, To> OneToMany<From, To> toMany(From from, Class<To> to)
    {
        return new OneToMany<From, To>();
    }
    
    public <From, To> OneToMany<From, To> toMany(From from, Class<To> to, String name)
    {
        return new OneToMany<From, To>();
    }
    
    public <Item> BinRedux<Item> bin(Class<Item> itemClass)
    {
        return new BinRedux<Item>();
    }
}
