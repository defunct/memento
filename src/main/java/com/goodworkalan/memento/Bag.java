package com.goodworkalan.memento;

import java.io.Serializable;

public class Bag
implements Serializable
{
    private static final long serialVersionUID = 20070210L;

    private final Long key;

    private final Long version;

    private final Object object;

    public Bag(Long key, Long version, Object object)
    {
        this.key = key;
        this.version = version;
        this.object = object;
    }

    public Long getKey()
    {
        return key;
    }

    public Object getObject()
    {
        return object;
    }

    public Long getVersion()
    {
        return version;
    }
}