/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import com.agtrz.swag.danger.Danger;

public class DepotException extends Danger
{
    private static final long serialVersionUID = 20070208L;
    
    public DepotException(Throwable cause)
    {
        super(cause);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */