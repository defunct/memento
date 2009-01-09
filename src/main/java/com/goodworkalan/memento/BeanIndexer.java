package com.goodworkalan.memento;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;



public class BeanIndexer<Item>
implements Indexer<Item, Ordered>
{
    private final String[] fields;

    private final Method[] getters;
    
    public BeanIndexer(String...fields)
    {
        Type superclass = getClass().getGenericSuperclass();

        if (superclass instanceof Class)
        {
            throw new RuntimeException("Missing type parameter.");
        }
        
        Type type = ((ParameterizedType) superclass).getActualTypeArguments()[0];
        Class<?> rawType = type instanceof Class<?> ? (Class<?>) type : (Class<?>) ((ParameterizedType) type).getRawType();

        this.fields = fields;
        this.getters = getGetters(rawType, fields);
    }
    
    public String[] getNames()
    {
        return fields;
    }

    private static Method[] getGetters(Class<?> rawType, String[] fields)
    {
        BeanInfo info;
        try
        {
            info = Introspector.getBeanInfo(rawType);
        }
        catch (IntrospectionException e)
        {
            throw new Danger("Introspection.", e, 100);
        }
        Method[] getters = new Method[fields.length];
        for (int i = 0; i < getters.length; i++)
        {
            for (int j = 0; j < info.getPropertyDescriptors().length; j++)
            {
                PropertyDescriptor property = info.getPropertyDescriptors()[j];
                if (property.getName().equals(fields[i]))
                {
                    getters[i] = property.getReadMethod();
                    break;
                }
            }
        }
        for (int i = 0; i < getters.length; i++)
        {
            if (getters[i] == null)
            {
                throw new Danger("Cannot find proeprty: " + fields[i], 300);
            }
        }
        return getters;
    }
    
    @SuppressWarnings("unchecked")
    public Ordered index(Item item)
    {
        Comparable[] comparables = new Comparable[getters.length];
        for (int i = 0; i < getters.length; i++)
        {
            try
            {
                comparables[i] = (Comparable) getters[i].invoke(item);
            }
            catch (Exception e)
            {
                throw new Danger("Unable to get property: " + fields[i], e, 300);
            }
        }
        return new Ordered(comparables);
    }
}