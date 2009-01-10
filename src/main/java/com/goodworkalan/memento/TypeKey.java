package com.goodworkalan.memento;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

abstract class TypeKey
{
    private final List<Object> key;
    
    protected TypeKey(List<Type> type, List<Object> other)
    {
        this.key = newKey(type, other);
    }
    
    protected TypeKey(List<Type> type)
    {
        this(type, Collections.emptyList());
    }
    
    protected TypeKey(Object...other)
    {
        List<Type> types = getActualTypeArguments(getClass());
        List<Object> others = Arrays.asList(other);
        this.key = newKey(types, others);
    }
    
    public static List<Type> getActualTypeArguments(Class<?> klass)
    {
        Type superClass = klass.getGenericSuperclass();  
        if (superClass instanceof Class)
        {  
            throw new RuntimeException("Missing Type Parameter");  
        }  
        ParameterizedType pt = (ParameterizedType) superClass;
        return Arrays.asList(pt.getActualTypeArguments());
    }
    
    private static List<Object> newKey(List<Type> type, List<Object> other)
    {
        List<Object> key = new ArrayList<Object>(type.size() + other.size());
        key.addAll(type);
        key.addAll(other);
        return key;
    }
    
    Object getHashKey()
    {
        return key;
    }
}
