/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import com.agtrz.swag.danger.Danger;

public class DepotException extends Danger
{
    private static final long serialVersionUID = 20070723L;
    
    public DepotException(String message, Throwable cause)
    {
        super(message, cause);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */