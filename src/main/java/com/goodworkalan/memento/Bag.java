package com.goodworkalan.memento;

import java.io.Serializable;

public class Bag
implements Serializable
{
    private static final long serialVersionUID = 20070210L;

    private final long key;

    private final long version;

    private final Object object;

    public Bag(long key, long version, Object object)
    {
        this.key = key;
        this.version = version;
        this.object = object;
    }

    public long getKey()
    {
        return key;
    }

    public long getVersion()
    {
        return version;
    }

    public Object getObject()
    {
        return object;
    }
}