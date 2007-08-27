/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;
import EDU.oswego.cs.dl.util.concurrent.Latch;
import EDU.oswego.cs.dl.util.concurrent.Sync;

public class UsageTestCase
extends TestCase
{
    private final static String RECIPIENTS = "recipients";

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

    private Depot newDepot(File file)
    {
        Depot.Creator creator = new Depot.Creator();
        Depot.Bin.Creator recipients = creator.newBin("recipients");
        Depot.Bin.Creator messages = creator.newBin("messages");
        Depot.Bin.Creator bounces = creator.newBin("bounces");

        recipients.newJoin("messages").add(messages).add(bounces);
        messages.newJoin("recipients").add(recipients).add(bounces);

        return creator.create(file);
    }

    public void testAddSingleRecord()
    {
        File file = newFile();
        Depot depot = newDepot(file);

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Depot.Snapshot snapshot = depot.newSnapshot();

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();
        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();
        Depot.Bag recipient = snapshot.getBin("recipients").add(marshaller, alan);

        Long key = recipient.getKey();

        assertEquals(alan, recipient.getObject());

        recipient = snapshot.getBin("recipients").get(unmarshaller, key);

        assertNotNull(recipient);
        assertEquals(alan, recipient.getObject());

        snapshot.commit();

        snapshot = depot.newSnapshot();

        recipient = snapshot.getBin("recipients").get(unmarshaller, key);

        assertNotNull(recipient);
        assertEquals(alan, recipient.getObject());

        snapshot.rollback();
    }

    public void testAddIsolation() throws InterruptedException
    {
        System.out.println("Test add isolation record.");
        File file = newFile();
        Depot depot = newDepot(file);

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Recipient bart = new Recipient("b@rox.com", "Bart", "Everson");

        Depot.Snapshot one = depot.newSnapshot();

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();
        final Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();
        Depot.Bag recipient = one.getBin("recipients").add(marshaller, alan);

        Long keyOfAlan = recipient.getKey();

        assertEquals(alan, recipient.getObject());

        Depot.Snapshot two = depot.newSnapshot();
        recipient = two.getBin("recipients").get(unmarshaller, keyOfAlan);

        assertNull(recipient);

        one.commit();

        System.out.println("One is done!");

        recipient = two.getBin("recipients").get(unmarshaller, keyOfAlan);
        assertNull(recipient);

        Depot.Test test = new Depot.Test();
        Sync changesWritten = new Latch();
        Sync registerMutation = new Latch();
        Sync committed = new Latch();
        test.setJournalLatches(changesWritten, registerMutation);
        test.setJournalComplete(committed);
        final Depot.Snapshot three = depot.newSnapshot(test);
        recipient = three.getBin("recipients").add(marshaller, bart);

        Long keyOfBart = recipient.getKey();

        assertEquals(bart, recipient.getObject());

        two = depot.newSnapshot();

        recipient = two.getBin("recipients").get(unmarshaller, keyOfAlan);

        assertNotNull(recipient);
        assertEquals(alan, recipient.getObject());

        recipient = two.getBin("recipients").get(unmarshaller, keyOfBart);
        assertNull(recipient);

        new Thread(new Runnable()
        {
            public void run()
            {
                System.out.println("Thread Running!");
                three.commit();
            }
        }).start();

        changesWritten.acquire();

        Depot.Snapshot four = depot.newSnapshot();

        recipient = four.getBin("recipients").get(unmarshaller, keyOfBart);
        assertNull(recipient);

        four.rollback();

        recipient = two.getBin("recipients").get(unmarshaller, keyOfBart);
        assertNull(recipient);

        registerMutation.release();
        committed.acquire();

        four = depot.newSnapshot();

        recipient = four.getBin("recipients").get(unmarshaller, keyOfBart);
        assertNotNull(recipient);
        assertEquals(bart, recipient.getObject());

        four.rollback();

        recipient = two.getBin("recipients").get(unmarshaller, keyOfBart);
        assertNull(recipient);

        two.rollback();
    }

    public void testAddTwoRecords()
    {
    }

    public void testUpdateRecord() throws InterruptedException
    {
        File file = newFile();
        Depot depot = newDepot(file);

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");

        Depot.Snapshot one = depot.newSnapshot();

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();
        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();
        Long key = one.getBin("recipients").add(marshaller, alan).getKey();
        one.commit();

        Thread.sleep(1);
        Depot.Snapshot two = depot.newSnapshot();
        Depot.Bag person = two.getBin("recipients").get(unmarshaller, key);

        assertNotNull(person);
        assertEquals(alan, person.getObject());

        Recipient kiloblog = new Recipient("alan@kiloblog.com", alan.getFirstName(), alan.getLastName());

        Thread.sleep(1);
        Depot.Snapshot three = depot.newSnapshot();
        Depot.Bag updated = three.getBin("recipients").update(marshaller, key, kiloblog);

        assertNotNull(updated);
        assertEquals(kiloblog, updated.getObject());

        Thread.sleep(1);
        Depot.Snapshot four = depot.newSnapshot();
        Depot.Bag previous = four.getBin(RECIPIENTS).get(unmarshaller, key);

        assertNotNull(previous);
        assertEquals(alan, previous.getObject());

        three.commit();

        previous = four.getBin(RECIPIENTS).get(unmarshaller, key);

        assertNotNull(previous);
        assertEquals(alan, previous.getObject());

        // four.rollback();

        Thread.sleep(1);
        Depot.Snapshot five = depot.newSnapshot();
        Depot.Bag next = five.getBin(RECIPIENTS).get(unmarshaller, key);
        depot.newSnapshot();

        assertNotNull(next);

        assertEquals(kiloblog, next.getObject());

        five.rollback();
    }

    public void testDeleteRecord()
    {
        File file = newFile();
        Depot depot = newDepot(file);

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");

        Depot.Snapshot one = depot.newSnapshot();

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();
        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Long key = one.getBin("recipients").add(marshaller, alan).getKey();
        one.commit();

        Depot.Snapshot two = depot.newSnapshot();
        Depot.Bag person = two.getBin("recipients").get(unmarshaller, key);

        assertNotNull(person);
        assertEquals(alan, person.getObject());

        Recipient kiloblog = new Recipient("alan@kiloblog.com", alan.getFirstName(), alan.getLastName());

        Depot.Snapshot three = depot.newSnapshot();
        Depot.Bag updated = three.getBin("recipients").update(marshaller, key, kiloblog);

        assertNotNull(updated);
        assertEquals(kiloblog, updated.getObject());

        Depot.Snapshot four = depot.newSnapshot();
        Depot.Bag previous = four.getBin(RECIPIENTS).get(unmarshaller, key);

        assertNotNull(previous);
        assertEquals(alan, previous.getObject());

        three.commit();

        previous = four.getBin(RECIPIENTS).get(unmarshaller, key);

        assertNotNull(previous);
        assertEquals(alan, previous.getObject());

        four.rollback();

        Depot.Snapshot five = depot.newSnapshot();
        Depot.Bag next = five.getBin(RECIPIENTS).get(unmarshaller, key);

        assertNotNull(next);
        assertEquals(kiloblog, next.getObject());

        Depot.Snapshot six = depot.newSnapshot();
        six.getBin(RECIPIENTS).delete(key);

        next = five.getBin(RECIPIENTS).get(unmarshaller, key);

        assertNotNull(next);
        assertEquals(kiloblog, next.getObject());

        six.commit();

        next = five.getBin(RECIPIENTS).get(unmarshaller, key);

        assertNotNull(next);
        assertEquals(kiloblog, next.getObject());

        five.rollback();

        Depot.Snapshot seven = depot.newSnapshot();
        Depot.Bag gone = seven.getBin(RECIPIENTS).get(unmarshaller, key);

        assertNull(gone);
    }

    public void testLink()
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();
        {
            Depot.Bin.Creator recipients = creator.newBin("recipients");
            Depot.Bin.Creator messages = creator.newBin("messages");
            Depot.Bin.Creator bounces = creator.newBin("bounces");

            recipients.newJoin("messages").add(messages).add(bounces);
            messages.newJoin("recipients").add(recipients).add(bounces);

            depot = creator.create(file);
        }

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Message hello = new Message("Hello, World!");
        Bounce received = new Bounce(false);

        Depot.Snapshot snapshot = depot.newSnapshot();

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();
        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Bag person = snapshot.getBin("recipients").add(marshaller, alan);
        Depot.Bag message = snapshot.getBin("messages").add(marshaller, hello);
        Depot.Bag bounce = snapshot.getBin("bounces").add(marshaller, received);

        person.link("messages", new Depot.Bag[] { message, bounce });

        Long keyOfPerson = person.getKey();

        Iterator linked = person.getLinked("messages");
        assertTrue(linked.hasNext());
        while (linked.hasNext())
        {
            Depot.Tuple tuple = (Depot.Tuple) linked.next();
            assertEquals(alan, tuple.getBag(unmarshaller, 0).getObject());
        }

        snapshot.commit();

        snapshot = depot.newSnapshot();

        person = snapshot.getBin("recipients").get(unmarshaller, keyOfPerson);
        linked = person.getLinked("messages");
        Depot.Tuple tuple = null;
        assertTrue(linked.hasNext());
        while (linked.hasNext())
        {
            tuple = (Depot.Tuple) linked.next();
            assertEquals(alan, tuple.getBag(unmarshaller, 0).getObject());
        }

        person.unlink("messages", new Depot.Bag[] { tuple.getBag(unmarshaller, 1), tuple.getBag(unmarshaller, 2) });
        linked = person.getLinked("messages");
        assertFalse(linked.hasNext());

        snapshot.commit();

        snapshot = depot.newSnapshot();

        person = snapshot.getBin("recipients").get(unmarshaller, keyOfPerson);
        linked = person.getLinked("messages");
        assertFalse(linked.hasNext());
    }

    private final static class FieldExtractor
    implements Depot.FieldExtractor
    {
        private static final long serialVersionUID = 20070403L;

        public Comparable[] getFields(Object object)
        {
            Recipient recipient = (Recipient) object;
            return new Comparable[] { recipient.getLastName(), recipient.getFirstName() };
        }
    }

    public void testIndex()
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();
        {
            Depot.Bin.Creator recipients = creator.newBin("recipients");
            recipients.newIndex("lastNameFirst", new FieldExtractor(), new Depot.SerializationUnmarshaller());
            depot = creator.create(file);
        }

        depot.newSnapshot().commit();
        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Recipient angelo = new Recipient("frank@thinknola.com", "Angelo", "Silvestri");
        Recipient frank = new Recipient("frank@thinknola.com", "Frank", "Silvestri");
        // Recipient bart = new Recipient("b@rox.com", "Bart", "Everson");
        // Recipient maitri = new Recipient("maitri.vr@gmail.com", "Maitri",
        // "Venkat-Ramani");
        //        
        // Message hello = new Message("Hello, World!");
        // Bounce received = new Bounce(false);
        //
        Depot.Snapshot snapshot = depot.newSnapshot();

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();

        Depot.Bag person = snapshot.getBin("recipients").add(marshaller, alan);
        assertEquals(alan, person.getObject());

        person = snapshot.getBin("recipients").add(marshaller, angelo);
        Iterator iterator = snapshot.getBin("recipients").find("lastNameFirst", new Comparable[] { "Silvestri" });

        assertTrue(iterator.hasNext());
        Depot.Bag bag = (Depot.Bag) iterator.next();
        assertEquals(angelo, bag.getObject());
        assertFalse(iterator.hasNext());

        snapshot.commit();

        snapshot = depot.newSnapshot();

        iterator = snapshot.getBin("recipients").find("lastNameFirst", new Comparable[] { "Silvestri" });

        assertTrue(iterator.hasNext());
        bag = (Depot.Bag) iterator.next();
        assertEquals(angelo, bag.getObject());
        assertFalse(iterator.hasNext());

        snapshot.getBin("recipients").update(marshaller, person.getKey(), frank);

        for (int i = 0; i < 0; i++)
        {
            System.gc();
        }

        snapshot.commit();

        snapshot = depot.newSnapshot();

        iterator = snapshot.getBin("recipients").find("lastNameFirst", new Comparable[] { "Silvestri" });

        assertTrue(iterator.hasNext());
        bag = (Depot.Bag) iterator.next();
        assertEquals(frank, bag.getObject());
        assertFalse(iterator.hasNext());
    }

    public void testUniqueIndex()
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();
        {
            Depot.Bin.Creator recipients = creator.newBin("recipients");
            Depot.Index.Creator newIndex = recipients.newIndex("lastNameFirst", new FieldExtractor(), new Depot.SerializationUnmarshaller());
            newIndex.setUnique(true);
            depot = creator.create(file);
        }

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();

        depot.newSnapshot().commit();

        Recipient alan1 = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Recipient alan2 = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Recipient angelo = new Recipient("angelo@thinknola.com", "Angelo", "Silvestri");
        Recipient frank = new Recipient("frank@thinknola.com", "Frank", "Silvestri");
        Recipient frank2 = new Recipient("frank@thinknola.com", "Angelo", "Silvestri");

        Depot.Snapshot snapshot = depot.newSnapshot();
        snapshot.getBin("recipients").add(marshaller, alan1);
        boolean exceptional = false;
        try
        {
            snapshot.getBin("recipients").add(marshaller, alan2);
        }
        catch (Depot.Error e)
        {
            assertEquals(Depot.UNIQUE_CONSTRAINT_VIOLATION_ERROR, e.code);
            exceptional = true;
        }
        assertTrue(exceptional);

        snapshot.getBin("recipients").add(marshaller, frank);
        snapshot.getBin("recipients").add(marshaller, angelo);
        exceptional = false;
        try
        {
            snapshot.getBin("recipients").add(marshaller, frank2);
        }
        catch (Depot.Error e)
        {
            assertEquals(Depot.UNIQUE_CONSTRAINT_VIOLATION_ERROR, e.code);
            exceptional = true;
        }
        assertTrue(exceptional);

        snapshot.commit();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */