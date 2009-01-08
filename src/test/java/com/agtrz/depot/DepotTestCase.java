/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.agtrz.depot.serializable.Person;

public class DepotTestCase
{
    private File newFile()
    {
        try
        {
            File file = File.createTempFile("momento", ".mto");
            file.deleteOnExit();
            return file;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    Person newPerson(String firstName, String lastName, String email)
    {
        Person person = new Person();
        person.setFirstName(firstName);
        person.setLastName(lastName);
        person.setEmail(email);
        return person;
    }
    
    @Test public void saveSingleObject()
    {
        File file = newFile();
        Creator creator = new Creator();
        Depot depot = creator.create(file);
        Person person = newPerson("Alan", "Gutierrez", "alan@blogometer.com");
        Snapshot snapshot = depot.newSnapshot();
        snapshot.add(person);
        long id = snapshot.getId(person);
        snapshot.commit();
        snapshot = depot.newSnapshot();
        person = snapshot.load(Person.class, id);
        snapshot.rollback();
        assertEquals("Alan", person.getFirstName());
    }
    
    @Test public void update()
    {
        File file = newFile();
        Creator creator = new Creator();
        Depot depot = creator.create(file);
        Person person = newPerson("Alan", "Gutierrez", "alan@blogometer.com");
        Snapshot snapshot = depot.newSnapshot();
        snapshot.add(person);
        long id = snapshot.getId(person);
        snapshot.commit();
        snapshot = depot.newSnapshot();
        person = snapshot.load(Person.class, id);
        snapshot.rollback();
        assertEquals("alan@blogometer.com", person.getEmail());
        snapshot = depot.newSnapshot();
        person.setEmail("alan@thinknola.com");
        snapshot.update(id, person);
        snapshot.commit();
        snapshot = depot.newSnapshot();
        person = snapshot.load(Person.class, id);
        snapshot.rollback();
        assertEquals("alan@thinknola.com", person.getEmail());
    }
    
    @Test public void delete()
    {
        File file = newFile();
        Creator creator = new Creator();
        Depot depot = creator.create(file);

        Person alan = newPerson("Alan", "Gutierrez", "alan@blogometer.com");

        Snapshot one = depot.newSnapshot();

        one.add(alan);
        long key = one.getId(alan);

        one.commit();

        Snapshot two = depot.newSnapshot();
        Person person = two.load(Person.class, key);

        assertNotNull(person);
        assertEquals(alan.getLastName(), person.getLastName());

        person.setEmail("alan@kiloblog.com");

        Snapshot three = depot.newSnapshot();
        three.update(key, person);
        person = three.load(Person.class, key);

        assertEquals("alan@kiloblog.com", person.getEmail());

        Snapshot four = depot.newSnapshot();
        Person previous = four.load(Person.class, key);

        assertNotNull(previous);
        assertEquals("alan@blogometer.com", previous.getEmail());

        three.commit();

        previous = four.load(Person.class, key);

        assertNotNull(previous);
        assertEquals("alan@blogometer.com", previous.getEmail());

        four.rollback();

        Snapshot five = depot.newSnapshot();
        person = five.load(Person.class, key);
        
        assertNotNull(person);
        assertEquals("alan@kiloblog.com",  person.getEmail());

        Snapshot six = depot.newSnapshot();
        six.delete(Person.class, key);

        Person next = five.load(Person.class, key);

        assertNotNull(next);
        assertEquals("alan@kiloblog.com",  next.getEmail());

        six.commit();

        next = five.load(Person.class, key);

        assertNotNull(next);
        assertEquals("alan@kiloblog.com", next.getEmail());

        five.rollback();

        Snapshot seven = depot.newSnapshot();
        Person gone = seven.load(Person.class, key);

        assertNull(gone);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */