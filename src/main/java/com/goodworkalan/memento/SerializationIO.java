package com.goodworkalan.memento;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class SerializationIO<T> implements ItemIO<T>
{
    private final Caster<T> itemClass;
    
    protected SerializationIO(Caster<T> itemClass)
    {
        this.itemClass = itemClass;
    }

    public void write(OutputStream out, T object)
    {
        try
        {
            new ObjectOutputStream(out).writeObject(object);
        }
        catch (IOException e)
        {
            throw new MementoException(113, e);
        }
    }
    
    public static <Type> SerializationIO<Type> getInstance(Class<Type> itemClass)
    {
        return new SerializationIO<Type>(new ClassCaster<Type>(itemClass));
    }
    
    public static <Type> SerializationIO<Type> getInstance(Item<Type> itemClass)
    {
        return null /* new SerializationIO<Type>(new UncheckedCast<Type>()) */;
    }

    static <Type> SerializationIO<Type> getInstance(Caster<Type> itemClass)
    {
        return new SerializationIO<Type>(itemClass);
    }

    public T read(InputStream in)
    {
        Object object;
        try
        {
            object = new ObjectInputStream(in).readObject();
        }
        catch (Exception e)
        {
            throw new MementoException(114, e);
        }
        return itemClass.cast(object);
    }
}
