/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.Serializable;

public class Bounce
implements Serializable
{
    private static final long serialVersionUID = 20070503L;

    private final boolean bounced;

    public Bounce(boolean bounced)
    {
        this.bounced = bounced;
    }

    public boolean getBounced()
    {
        return bounced;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */