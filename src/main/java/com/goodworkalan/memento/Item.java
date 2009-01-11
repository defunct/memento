package com.goodworkalan.memento;

import java.util.Collections;

public abstract class Item<T> extends TypeKey
{
    Item(Class<T> type)
    {
        super(Collections.singletonList(type));
    }
    
    public Item()
    {
        super();
    }
}
