package com.goodworkalan.memento;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;



public final class SerializationUnmarshaller
implements Unmarshaller, Serializable
{
    private static final long serialVersionUID = 20070826L;

    public Object unmarshall(InputStream in)
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
        return object;
    }
}