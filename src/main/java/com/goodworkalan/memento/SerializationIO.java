package com.goodworkalan.memento;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class SerializationIO<Item> implements ItemIO<Item>
{
    private final Class<Item> itemClass;
    
    protected SerializationIO(Class<Item> itemClass)
    {
        this.itemClass = itemClass;
    }

    public void write(OutputStream out, Item object)
    {
        try
        {
            new ObjectOutputStream(out).writeObject(object);
        }
        catch (IOException e)
        {
            throw new Danger("io", 403);
        }
    }
    
    public static <Type> SerializationIO<Type> getInstance(Class<Type> itemClass)
    {
        return new SerializationIO<Type>(itemClass);
    }

    public Item read(InputStream in)
    {
        Object object;
        try
        {
            object = new ObjectInputStream(in).readObject();
        }
        catch (IOException e)
        {
            throw new Danger("io", 403);
        }
        catch (ClassNotFoundException e)
        {
            throw new Danger("class.not.found", 403);
        }
        return itemClass.cast(object);
    }
}
