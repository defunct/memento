/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.Serializable;

public class Circle
implements Serializable
{
    private static final long serialVersionUID = 20070208L;

    private final String name;

    public Circle(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public boolean equals(Object object)
    {
        if (object instanceof Circle)
        {
            Circle person = (Circle) object;
            return name.equals(person.getName());
        }
        return false;
    }

    public int hashCode()
    {
        return getName().hashCode();
    }

    public String toString()
    {
        return getName();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */