package com.goodworkalan.memento;

import java.lang.reflect.Type;

public class IndexKey
{
    private final Type type;
    
    private final String name;
    
    public IndexKey(Type type, String name)
    {
        this.type = type;
        this.name = name;
    }
}
