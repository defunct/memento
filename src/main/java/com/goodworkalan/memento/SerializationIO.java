package com.goodworkalan.memento;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import com.goodworkalan.ilk.Ilk;

// TODO Document.
public class SerializationIO<T> implements ItemIO<T>
{
    // TODO Document.
    private final Ilk<T> ilk;
    
    // TODO Document.
    protected SerializationIO(Ilk<T> ilk)
    {
        this.ilk = ilk;
    }

    // TODO Document.
    public static <Type> SerializationIO<Type> getInstance(Class<Type> itemClass)
    {
        return new SerializationIO<Type>(new Ilk<Type>(itemClass));
    }
    
    // TODO Document.
    public static <Type> SerializationIO<Type> getInstance(Ilk<Type> ilk)
    {
        return new SerializationIO<Type>(ilk);
    }

    // TODO Document.
    public void write(OutputStream out, T object)
    {
        try
        {
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(ilk.pair(object));
            oos.flush();
        }
        catch (IOException e)
        {
            throw new MementoException(MementoException.BOGUS_EXCEPTION_THROWN_BY_LOSER_BOY, e);
        }
    }

    // TODO Document.
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
        if (object instanceof Ilk.Pair)
        {
            return ((Ilk.Pair) object).cast(ilk);
        }
        throw new MementoException(MementoException.BOGUS_EXCEPTION_THROWN_BY_LOSER_BOY);
    }
}
