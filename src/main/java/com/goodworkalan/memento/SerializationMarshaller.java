package com.goodworkalan.memento;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import com.agtrz.depot.Danger;


public final class SerializationMarshaller
implements Marshaller, Serializable
{
    private static final long serialVersionUID = 20070826L;

    public void marshall(OutputStream out, Object object)
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
}