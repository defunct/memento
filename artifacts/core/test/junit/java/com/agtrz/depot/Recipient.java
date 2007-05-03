/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.Serializable;

public class Recipient
implements Serializable
{
    private static final long serialVersionUID = 20070412L;

    private final String email;

    private final String firstName;

    private final String lastName;

    public Recipient(String email, String firstName, String lastName)
    {
        this.email = email;
        this.firstName = firstName;
        this.lastName = lastName;
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

    public boolean equals(Object object)
    {
        if (object instanceof Recipient)
        {
            Recipient recipient = (Recipient) object;
            return lastName.equals(recipient.lastName) && firstName.equals(recipient.firstName) && email.equals(recipient.email);
        }
        return false;
    }

    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + lastName.hashCode();
        hashCode = hashCode * 37 + firstName.hashCode();
        hashCode = hashCode * 37 + email.hashCode();
        return hashCode;
    }
    
    public String toString()
    {
        return firstName + " " + lastName + " <" + email + ">";
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */