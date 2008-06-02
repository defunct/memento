/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.Serializable;

public class Message
implements Serializable
{
    private static final long serialVersionUID = 20070503L;

    private final String subject;
    
    public Message(String subject)
    {
        this.subject = subject;
    }
    
    public String getSubject()
    {
        return subject;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */