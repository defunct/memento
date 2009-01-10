package com.goodworkalan.memento;

import java.lang.reflect.Type;
import java.util.Collections;

public abstract class Index<T> extends TypeKey
{
    Index(Type type, String name)
    {
        super(Collections.singletonList(type), Collections.singletonList(name));
    }
    
    public Index(String name)
    {
        super(name);
    }
}
