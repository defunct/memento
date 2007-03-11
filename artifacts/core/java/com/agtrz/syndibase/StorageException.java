/* Copyright Alan Gutierrez 2006 */
package com.agtrz.syndibase;

import com.agtrz.swag.danger.Danger;

public class StorageException extends Danger
{
    private static final long serialVersionUID = 20070208L;
    
    public StorageException(Throwable cause)
    {
        super(cause);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */