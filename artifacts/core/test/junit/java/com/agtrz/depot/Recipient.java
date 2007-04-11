/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

public class Recipient
{
    private final String email;
    
    private final String firstName;
    
    private final String lastName;
    
    public Recipient(String email, String firstName, String lastName)
    {
        this.email = email;
        this.firstName = firstName;
        this.lastName= lastName;
    }
    
    public String getEmail()
    {
        return email;
    }
    
    public String getFirstName()
    {
        return firstName;
    }
    
    public String getLastName()
    {
        return lastName;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */