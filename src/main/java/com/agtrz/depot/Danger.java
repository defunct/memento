package com.agtrz.depot;

import java.util.ArrayList;
import java.util.List;

public final class Danger
extends RuntimeException
{
    private static final long serialVersionUID = 20070210L;

    public final int code;

    public final List<Object> listOfParameters;

    public Danger(String message, Throwable cause, int code)
    {
        super(message, cause);
        this.code = code;
        this.listOfParameters = new ArrayList<Object>();
    }

    public Danger(String message, int code)
    {
        super(message);
        this.code = code;
        this.listOfParameters = new ArrayList<Object>();
    }

    public Danger add(Object parameter)
    {
        listOfParameters.add(parameter);
        return this;
    }
}