/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.File;
import java.io.IOException;

import com.agtrz.depot.Depot;

import junit.framework.TestCase;

public class DepotTestCaseX
extends TestCase
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

    public void testCreateBag()
    {
        Depot.Creator creator = new Depot.Creator();
        creator.newBin("people");
        File file = newFile();
        Depot storage = creator.create(file);

        Test.Environment env = new Test.Environment(file, storage);
        new Test.Add(new Test.PersonServer("Dwight", "Eisenhower"), "people").operate(env);
        new Test.Get(0).operate(env);
    }

    public void testCreateTwoBags()
    {
        Depot.Creator creator = new Depot.Creator();
        creator.newBin("people");
        creator.newBin("circles");
        File file = newFile();
        Depot storage = creator.create(file);

        Test.Environment env = new Test.Environment(file, storage);
        new Test.Add(new Test.PersonServer("Dwight", "Eisenhower"), "people").operate(env);
        new Test.Get(0).operate(env);
        new Test.Add(new Test.CircleServer("United States Army"), "circles").operate(env);
        new Test.Get(0).operate(env);
        new Test.Get(1).operate(env);
    }

    public void testTwoItemsInBag()
    {
        Depot.Creator creator = new Depot.Creator();
        creator.newBin("people");
        File file = newFile();
        Depot storage = creator.create(file);

        Test.Environment env = new Test.Environment(file, storage);
        new Test.Add(new Test.PersonServer("Dwight", "Eisenhower"), "people").operate(env);
        new Test.Get(0).operate(env);
        new Test.Add(new Test.PersonServer("John", "Kennedy"), "people").operate(env);
        new Test.Get(0).operate(env);
        new Test.Get(1).operate(env);
    }

    public void testReopenOneBag()
    {
        Depot.Creator creator = new Depot.Creator();
        creator.newBin("people");
        File file = newFile();
        Depot storage = creator.create(file);

        Test.Environment env = new Test.Environment(file, storage);
        new Test.Add(new Test.PersonServer("Dwight", "Eisenhower"), "people").operate(env);
        new Test.Get(0).operate(env);
        new Test.Commit().operate(env);

        env.reopen();
        new Test.Get(0).operate(env);
    }

    public void testReopenTwoBags()
    {
        Depot.Creator creator = new Depot.Creator();
        creator.newBin("people");
        creator.newBin("circles");
        File file = newFile();
        Depot storage = creator.create(file);

        Test.Environment env = new Test.Environment(file, storage);
        new Test.Add(new Test.PersonServer("Dwight", "Eisenhower"), "people").operate(env);
        new Test.Get(0).operate(env);
        new Test.Add(new Test.CircleServer("United States Army"), "circles").operate(env);
        new Test.Get(0).operate(env);
        new Test.Get(1).operate(env);
        new Test.Commit().operate(env);

        env.reopen();
        new Test.Get(0).operate(env);
        new Test.Get(1).operate(env);
    }

    public void testReopenTwoItemsInBag()
    {
        Depot.Creator creator = new Depot.Creator();
        creator.newBin("people");
        File file = newFile();
        Depot storage = creator.create(file);

        Test.Environment env = new Test.Environment(file, storage);
        new Test.Add(new Test.PersonServer("Dwight", "Eisenhower"), "people").operate(env);
        new Test.Get(0).operate(env);
        new Test.Add(new Test.PersonServer("John", "Kennedy"), "people").operate(env);
        new Test.Get(0).operate(env);
        new Test.Get(1).operate(env);
        new Test.Commit().operate(env);

        env.reopen();
        new Test.Get(0).operate(env);
        new Test.Get(1).operate(env);
    }

    public void testRollback()
    {
        Depot.Creator creator = new Depot.Creator();
        creator.newBin("people");
        File file = newFile();
        Depot storage = creator.create(file);

        Test.Environment env = new Test.Environment(file, storage);
        new Test.Add(new Test.PersonServer("Dwight", "Eisenhower"), "people").operate(env);
        new Test.Rollback().operate(env);
        new Test.RolledBack(0).operate(env);
    }

    public void testRelate()
    {
        File file = newFile();
        Depot storage = null;

        {
            Depot.Creator creator = new Depot.Creator();
            Depot.Bin.Creator people = creator.newBin("people");
            Depot.Bin.Creator circles = creator.newBin("circles");
            Depot.Join.Creator membership = creator.newJoin("membership").add(circles.getName()).add(people.getName());
            membership.alternate(new String[] { "people" });
            storage = creator.create(file);
        }

        Test.Environment env = new Test.Environment(file, storage);
        new Test.Add(new Test.PersonServer("Dwight", "Eisenhower"), "people").operate(env);
        new Test.Get(0).operate(env);
        new Test.Add(new Test.CircleServer("United States Army"), "circles").operate(env);
        new Test.Join("membership", 0, 1).operate(env);
        new Test.Join("membership", 1, 0).operate(env);
        new Test.Get(0).operate(env);
        new Test.Get(1).operate(env);
        new Test.Commit().operate(env);

        env.reopen();
        new Test.Get(0).operate(env);
        new Test.Get(1).operate(env);
    }

    // private Storage newStorage()
    // {
    // Storage.Creator creator = new Storage.Creator();
    // creator.newBag("people");
    // File file = newFile();
    // Storage storage = creator.create(file);
    // return storage;
    // }

    public void testCreate()
    {
        // Storage.Creator creator = new Storage.Creator();
        //
        // Storage.BagCreator circles = creator.newBag("circles");
        // circles.addIndex("name", new Strata.FieldExtractor()
        // {
        // public Object[] getFields(Object object)
        // {
        // return new Object[] { ((Circle) object).getName() };
        // }
        // });
        //
        // Storage.BagCreator people = creator.newBag("people");
        // people.addIndex("last-name-first", new Strata.FieldExtractor()
        // {
        // public Object[] getFields(Object object)
        // {
        // Person person = (Person) object;
        // return new Object[] { person.getLastName(), person.getFirstName()
        // };
        // }
        // });
        //
        // // Storage.RelationshipCreator membership =
        // // creator.newRelationship("membership");
        //
        // File file = newFile();
        // Storage storage = creator.create(file);
        //
        // // Circle circle = new Circle("Think New Orleans");
        //
        // Person person = new Person();
        // person.setFirstName("Alan");
        // person.setLastName("Gutierrez");
        //
        // // Record keptCircle = storage.add(circle);
        // Record keptPerson = storage.add(person);
        //
        // // Storage.Relationship membership =
        // // storage.getRelationship("membership");
        // // membership.relate(keptPerson, keptCircle);
        //
        // // storage.close();
        // //
        // // Storage.Opener opener = new Storage.Opener(file);
        // //
        // // storage = opener.open();
        //
        // keptPerson = storage.get(keptPerson.getKey());
        //
        // person = (Person) keptPerson.getObject();
        //
        // assertEquals("Alan", person.getFirstName());
        // assertEquals("Gutierrez", person.getLastName());
        //
        // // Iterator related = new
        // // RecordIterator(keptPerson.related("belongs-to"));
        // //
        // // assertTrue(related.hasNext());
        // // circle = (Circle) related.next();
        // // assertEquals("Think New Orleans", circle.getName());
        // // assertFalse(related.hasNext());
    }

    public void testOpen()
    {

    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */