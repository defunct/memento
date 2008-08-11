/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot.serializable;

import java.io.Serializable;

public class Person
implements Serializable
{
    private static final long serialVersionUID = 20070208L;

    private String firstName;

    private String lastName;

    private String email;
    
    public String getFirstName()
    {
        return firstName;
    }

    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    public String getLastName()
    {
        return lastName;
    }

    public void setLastName(String lastName)
    {
        this.lastName = lastName;
    }
    
    public String getEmail()
    {
        return email;
    }
    
    public void setEmail(String email)
    {
        this.email = email;
    }

    public boolean equals(Object object)
    {
        if (object instanceof Person)
        {
            Person person = (Person) object;
            return lastName.equals(person.getLastName()) && firstName.equals(person.getFirstName());
        }
        return false;
    }

    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + getLastName().hashCode();
        hashCode = hashCode * 37 + getFirstName().hashCode();
        return hashCode;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */