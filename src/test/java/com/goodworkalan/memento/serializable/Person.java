/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.memento.serializable;

import java.io.Serializable;

/**
 * An example person string bean.
 *
 * @author Alan Gutierrez
 */
public class Person
implements Serializable
{
    /** The serial version id. */
    private static final long serialVersionUID = 20070208L;

    /** The first name. */
    public String firstName;

    /** The last name. */
    public String lastName;

    /** The email address. */
    public String email;
  
    public boolean equals(Object object) {
        if (object instanceof Person) {
            Person person = (Person) object;
            return lastName.equals(person.lastName)
                    && firstName.equals(person.firstName);
        }
        return false;
    }

    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + lastName.hashCode();
        hashCode = hashCode * 37 + firstName.hashCode();
        return hashCode;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */