package com.agtrz.depot;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.goodworkalan.memento.Indexer;


public class BeanIndexer<T>
implements Indexer<T>
{
    private static final long serialVersionUID = 20080614L;

    private Class<?> rawType;
    
    private String[] fields;

    private transient Method[] getters;
    
    public BeanIndexer(String... fields)
    {
        Type superclass = getClass().getGenericSuperclass();
        if (superclass instanceof Class)
        {
            throw new RuntimeException("Missing type parameter.");
        }
        Type type = ((ParameterizedType) superclass).getActualTypeArguments()[0];
        Class<?> rawType = type instanceof Class<?> ? (Class<?>) type : (Class<?>) ((ParameterizedType) type).getRawType();

        this.rawType = rawType;
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
    
    public Comparable<?>[] extract(T type)
    {
        Comparable<?>[] index = new Comparable<?>[getters.length];
        for (int i = 0; i < getters.length; i++)
        {
            try
            {
                Comparable<?> comparable = (Comparable<?>) getters[i].invoke(type);
                index[i] = comparable;
            }
            catch (Exception e)
            {
                throw new Danger("Unable to get property: " + fields[i], e, 300);
            }
        }
        return index;
    }

    private void writeObject(ObjectOutputStream stream) throws IOException
    {
        stream.writeObject(rawType);
        stream.writeObject(fields);
    }
    
     private void readObject(java.io.ObjectInputStream stream)
         throws IOException, ClassNotFoundException
     {
         rawType = (Class<?>) stream.readObject();
         fields = (String[]) stream.readObject();
         getters = getGetters(rawType, fields);
     }
}