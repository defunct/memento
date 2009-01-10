package com.goodworkalan.memento;

public class ClassCaster<Item> implements Cast<Item>
{
    private final Class<Item> itemClass;
    
    public ClassCaster(Class<Item> itemClass)
    {
        this.itemClass = itemClass;
    }
    
    public Item cast(Object object)
    {
        return itemClass.cast(object);
    }
}
