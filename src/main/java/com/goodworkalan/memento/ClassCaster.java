package com.goodworkalan.memento;

public class ClassCaster<T> implements Caster<T>
{
    private final Class<T> itemClass;
    
    public ClassCaster(Class<T> itemClass)
    {
        this.itemClass = itemClass;
    }
    
    public T cast(Object object)
    {
        return itemClass.cast(object);
    }
}
