package com.goodworkalan.memento;

public class TypeCaster<Item> implements Caster<Item>
{
    @SuppressWarnings("unchecked")
    public Item cast(Object object)
    {
        return (Item) object;
    }
}
