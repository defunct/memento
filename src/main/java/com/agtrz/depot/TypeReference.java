package com.agtrz.depot;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * References a generic type.
 * 
 * @author crazybob@google.com (Bob Lee)
 */
public abstract class TypeReference<T>
{

    private final Type type;

    private volatile Constructor<?> constructor;

    protected TypeReference()
    {
        Type superclass = getClass().getGenericSuperclass();
        if (superclass instanceof Class)
        {
            throw new RuntimeException("Missing type parameter.");
        }
        this.type = ((ParameterizedType) superclass).getActualTypeArguments()[0];
    }

    /**
     * Instantiates a new instance of {@code T} using the default, no-arg
     * constructor.
     */
    @SuppressWarnings("unchecked")
    public T newInstance() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
    {
        if (constructor == null)
        {
            Class<?> rawType = getRawType();
            constructor = rawType.getConstructor();
        }
        return (T) constructor.newInstance();
    }

    /**
     * Gets the referenced type.
     */
    public Type getType()
    {
        return this.type;
    }
    
    /** 
     * Gets the class or the raw type of a parameterized type.
     * 
     * @return A class.
     */
    public Class<?> getRawType()
    {
        return type instanceof Class<?>
             ? (Class<?>) type
             : (Class<?>) ((ParameterizedType) type).getRawType();
    }
}