package com.goodworkalan.memento;

import java.util.Collections;

public abstract class Item<T> extends TypeKey
{
    final Caster<T> caster;
    
    Item(Class<T> type)
    {
        super(Collections.singletonList(type));
        this.caster = new ClassCaster<T>(type);
    }
    
    public Item()
    {
        super();
        this.caster = new UnsafeCast<T>();
    }
}
