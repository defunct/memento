package com.goodworkalan.memento;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class Error
extends RuntimeException
{
    private static final long serialVersionUID = 20070210L;

    private final Map<String, Object> mapOfProperties;

    public final int code;

    public Error(String message, int code)
    {
        super(message);
        this.code = code;
        this.mapOfProperties = new HashMap<String, Object>();
    }

    public Error(String message, int code, Throwable cause)
    {
        super(message, cause);
        this.code = code;
        this.mapOfProperties = new HashMap<String, Object>();
    }

    public Error put(String name, Object value)
    {
        mapOfProperties.put(name, value);
        return this;
    }

    public Map<String, Object> getProperties()
    {
        return Collections.unmodifiableMap(mapOfProperties);
    }
}