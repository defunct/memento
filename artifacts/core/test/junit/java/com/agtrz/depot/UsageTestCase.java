/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

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

        Depot.Join.Creator newJoin = creator.newJoin("messages");

        newJoin.add(recipients.getName());
        newJoin.add(messages.getName());
        newJoin.add(bounces.getName());

        newJoin.alternate(new String[] { "messages", "recipients", "bounces" });

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

        recipient = two.getBin("recipients").get(unmarshaller, keyOfAlan);
        assertNull(recipient);

        Depot.Test test = Depot.newTest();
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
                three.commit();
            }
        }).start();

        test.waitForChangesWritten();

        Depot.Snapshot four = depot.newSnapshot();

        recipient = four.getBin("recipients").get(unmarshaller, keyOfBart);
        assertNull(recipient);

        four.rollback();

        recipient = two.getBin("recipients").get(unmarshaller, keyOfBart);
        assertNull(recipient);

        test.registerMutation();
        test.waitForCompletion();

        four = depot.newSnapshot();

        recipient = four.getBin("recipients").get(unmarshaller, keyOfBart);
        assertNotNull(recipient);
        assertEquals(bart, recipient.getObject());

        four.rollback();

        recipient = two.getBin("recipients").get(unmarshaller, keyOfBart);
        assertNull(recipient);

        two.rollback();
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

    public void testJoin()
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();
        {
            Depot.Bin.Creator recipients = creator.newBin("recipients");
            Depot.Bin.Creator messages = creator.newBin("messages");
            Depot.Bin.Creator bounces = creator.newBin("bounces");

            Depot.Join.Creator newJoin = creator.newJoin("messages");

            newJoin.add(recipients.getName());
            newJoin.add(messages.getName());
            newJoin.add(bounces.getName());

            newJoin.alternate(new String[] { "messages", "recipients", "bounces" });

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

        Map select = new HashMap();

        select.put("recipients", person.getKey());
        select.put("messages", message.getKey());
        select.put("bounces", bounce.getKey());

        snapshot.getJoin("messages").link(select);

        Long keyOfPerson = person.getKey();

        select.clear();

        select.put("recipients", person.getKey());
        Iterator linked = snapshot.getJoin("messages").find(select);

        assertTrue(linked.hasNext());
        while (linked.hasNext())
        {
            Depot.Tuple tuple = (Depot.Tuple) linked.next();
            assertEquals(alan, tuple.getBag(unmarshaller, "recipients").getObject());
        }

        snapshot.commit();

        snapshot = depot.newSnapshot();

        person = snapshot.getBin("recipients").get(unmarshaller, keyOfPerson);

        select.clear();
        select.put("recipients", person.getKey());
        linked = snapshot.getJoin("messages").find(select);

        Depot.Tuple tuple = null;
        assertTrue(linked.hasNext());
        while (linked.hasNext())
        {
            tuple = (Depot.Tuple) linked.next();
            assertEquals(alan, tuple.getBag(unmarshaller, "recipients").getObject());
        }

        select.clear();

        select.put("recipients", tuple.getBag(unmarshaller, "recipients").getKey());
        select.put("messages", tuple.getBag(unmarshaller, "messages").getKey());
        select.put("bounces", tuple.getBag(unmarshaller, "bounces").getKey());

        snapshot.getJoin("messages").unlink(select);

        select.clear();
        select.put("recipients", person.getKey());
        linked = snapshot.getJoin("messages").find(select);

        assertFalse(linked.hasNext());

        snapshot.commit();

        snapshot = depot.newSnapshot();

        person = snapshot.getBin("recipients").get(unmarshaller, keyOfPerson);

        select.clear();
        select.put("recipients", person.getKey());
        linked = snapshot.getJoin("messages").find(select);

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
        assertEquals(angelo, iterator.next());
        assertFalse(iterator.hasNext());

        snapshot.commit();

        snapshot = depot.newSnapshot();

        iterator = snapshot.getBin("recipients").find("lastNameFirst", new Comparable[] { "Silvestri" });

        assertTrue(iterator.hasNext());
        assertEquals(angelo, iterator.next());
        assertFalse(iterator.hasNext());

        snapshot.getBin("recipients").update(marshaller, person.getKey(), frank);

        snapshot.commit();

        snapshot = depot.newSnapshot();

        iterator = snapshot.getBin("recipients").find("lastNameFirst", new Comparable[] { "Silvestri" });

        assertTrue(iterator.hasNext());
        assertEquals(frank, iterator.next());
        assertFalse(iterator.hasNext());

        snapshot.rollback();

        depot.close();

        Depot.Opener opener = new Depot.Opener();
        depot = opener.open(file);

        snapshot = depot.newSnapshot();

        iterator = snapshot.getBin("recipients").find("lastNameFirst", new Comparable[] { "Silvestri" });

        assertTrue(iterator.hasNext());
        assertEquals(frank, iterator.next());
        assertFalse(iterator.hasNext());

        snapshot.rollback();

        depot.close();
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

        depot.close();

        Depot.Opener opener = new Depot.Opener();
        depot = opener.open(file);

        snapshot = depot.newSnapshot();

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

    public void testUniqueIndexConcurrentModificationCommit()
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

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");

        Depot.Snapshot one = depot.newSnapshot();
        one.getBin("recipients").add(marshaller, alan);

        Depot.Snapshot two = depot.newSnapshot();
        two.getBin("recipients").add(marshaller, alan);

        two.commit();

        try
        {
            one.commit();
        }
        catch (Depot.Error e)
        {
            assertEquals(Depot.CONCURRENT_MODIFICATION_ERROR, e.code);
            return;
        }
        fail("Expected exception not thrown.");
    }

    public void testUniqueIndexConcurrentModification()
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

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");

        Depot.Snapshot one = depot.newSnapshot();
        one.getBin("recipients").add(marshaller, alan);

        Depot.Test test = Depot.newTest();
        final Depot.Snapshot two = depot.newSnapshot(test);
        two.getBin("recipients").add(marshaller, alan);

        new Thread(new Runnable()
        {
            public void run()
            {
                two.commit();
            }
        }).start();

        test.waitForChangesWritten();

        try
        {
            one.commit();
        }
        catch (Depot.Error e)
        {
            assertEquals(Depot.CONCURRENT_MODIFICATION_ERROR, e.code);
            return;
        }
        fail("Expected exception not thrown.");
    }

    public void testConcurrentBinModificationCommit()
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();

        {
            creator.newBin("recipients");
            depot = creator.create(file);
        }

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();

        Recipient alan1 = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");

        Depot.Snapshot one = depot.newSnapshot();
        Depot.Bag person = one.getBin("recipients").add(marshaller, alan1);

        one.commit();

        Recipient alan2 = new Recipient("alan@kiloblog.com", "Alan", "Gutierrez");

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Snapshot two = depot.newSnapshot();
        person = two.getBin("recipients").get(unmarshaller, person.getKey());
        two.getBin("recipients").update(marshaller, person.getKey(), alan2);

        Depot.Snapshot three = depot.newSnapshot();
        person = three.getBin("recipients").get(unmarshaller, person.getKey());
        three.getBin("recipients").update(marshaller, person.getKey(), alan2);

        three.commit();

        try
        {
            two.commit();
        }
        catch (Depot.Error e)
        {
            assertEquals(Depot.CONCURRENT_MODIFICATION_ERROR, e.code);
            return;
        }
        fail("Expected exception not thrown.");
    }

    public void testConcurrentBinModificationChanges()
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();

        {
            creator.newBin("recipients");
            depot = creator.create(file);
        }

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();

        Recipient alan1 = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");

        Depot.Snapshot one = depot.newSnapshot();
        Depot.Bag person = one.getBin("recipients").add(marshaller, alan1);

        one.commit();

        Recipient alan2 = new Recipient("alan@kiloblog.com", "Alan", "Gutierrez");

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Snapshot two = depot.newSnapshot();
        person = two.getBin("recipients").get(unmarshaller, person.getKey());
        two.getBin("recipients").update(marshaller, person.getKey(), alan2);

        Depot.Test test = Depot.newTest();
        final Depot.Snapshot three = depot.newSnapshot(test);
        person = three.getBin("recipients").get(unmarshaller, person.getKey());
        three.getBin("recipients").update(marshaller, person.getKey(), alan2);

        new Thread(new Runnable()
        {
            public void run()
            {
                three.commit();
            }
        }).start();

        test.waitForChangesWritten();

        for (;;)
        {
            try
            {
                two.commit();
            }
            catch (Depot.Error e)
            {
                assertEquals(Depot.CONCURRENT_MODIFICATION_ERROR, e.code);
                break;
            }
            fail("Expected exception not thrown.");
        }

        test.registerMutation();
        test.waitForCompletion();
    }

    public void testConcurrentJoinModificationCommit()
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();
        {
            Depot.Bin.Creator recipients = creator.newBin("recipients");
            Depot.Bin.Creator messages = creator.newBin("messages");
            Depot.Bin.Creator bounces = creator.newBin("bounces");

            creator.newJoin("messages").add(recipients.getName()).add(messages.getName()).add(bounces.getName());
            creator.newJoin("recipients").add(messages.getName()).add(recipients.getName()).add(bounces.getName());

            depot = creator.create(file);
        }

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Message hello = new Message("Hello, World!");
        Bounce received = new Bounce(false);

        Depot.Snapshot one = depot.newSnapshot();

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();
        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Bag person = one.getBin("recipients").add(marshaller, alan);
        Depot.Bag message = one.getBin("messages").add(marshaller, hello);
        Depot.Bag bounce = one.getBin("bounces").add(marshaller, received);

        one.commit();

        Depot.Snapshot two = depot.newSnapshot();

        person = two.getBin("recipients").get(unmarshaller, person.getKey());
        message = two.getBin("messages").get(unmarshaller, message.getKey());
        bounce = two.getBin("bounces").get(unmarshaller, bounce.getKey());

        Map select = new HashMap();

        select.put("recipients", person.getKey());
        select.put("messages", message.getKey());
        select.put("bounces", bounce.getKey());

        two.getJoin("messages").link(select);

        Depot.Test test = Depot.newTest();
        final Depot.Snapshot three = depot.newSnapshot(test);

        person = three.getBin("recipients").get(unmarshaller, person.getKey());
        message = three.getBin("messages").get(unmarshaller, message.getKey());
        bounce = three.getBin("bounces").get(unmarshaller, bounce.getKey());

        select.clear();

        select.put("recipients", person.getKey());
        select.put("messages", message.getKey());
        select.put("bounces", bounce.getKey());

        three.getJoin("messages").link(select);

        new Thread(new Runnable()
        {
            public void run()
            {
                three.commit();
            }
        }).start();

        test.waitForChangesWritten();

        try
        {
            two.commit();
        }
        catch (Depot.Error e)
        {
            assertEquals(Depot.CONCURRENT_MODIFICATION_ERROR, e.code);
            return;
        }
        fail("Expected exception not thrown.");
    }

    public void testConcurrentJoinModificationChanges()
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();
        {
            Depot.Bin.Creator recipients = creator.newBin("recipients");
            Depot.Bin.Creator messages = creator.newBin("messages");
            Depot.Bin.Creator bounces = creator.newBin("bounces");

            creator.newJoin("messages").add(recipients.getName()).add(messages.getName()).add(bounces.getName());
            creator.newJoin("recipients").add(messages.getName()).add(recipients.getName()).add(bounces.getName());

            depot = creator.create(file);
        }

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Message hello = new Message("Hello, World!");
        Bounce received = new Bounce(false);

        Depot.Snapshot one = depot.newSnapshot();

        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();
        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Bag person = one.getBin("recipients").add(marshaller, alan);
        Depot.Bag message = one.getBin("messages").add(marshaller, hello);
        Depot.Bag bounce = one.getBin("bounces").add(marshaller, received);

        one.commit();

        Depot.Snapshot two = depot.newSnapshot();

        person = two.getBin("recipients").get(unmarshaller, person.getKey());
        message = two.getBin("messages").get(unmarshaller, message.getKey());
        bounce = two.getBin("bounces").get(unmarshaller, bounce.getKey());

        Map select = new HashMap();

        select.put("recipients", person.getKey());
        select.put("messages", message.getKey());
        select.put("bounces", bounce.getKey());

        two.getJoin("messages").link(select);

        Depot.Snapshot three = depot.newSnapshot();

        person = three.getBin("recipients").get(unmarshaller, person.getKey());
        message = three.getBin("messages").get(unmarshaller, message.getKey());
        bounce = three.getBin("bounces").get(unmarshaller, bounce.getKey());

        select.clear();

        select.put("recipients", person.getKey());
        select.put("messages", message.getKey());
        select.put("bounces", bounce.getKey());

        three.getJoin("messages").link(select);

        three.commit();

        try
        {
            two.commit();
        }
        catch (Depot.Error e)
        {
            assertEquals(Depot.CONCURRENT_MODIFICATION_ERROR, e.code);
            return;
        }
        fail("Expected exception not thrown.");
    }

    // public void testThought()
    // {
    // for (;;)
    // {
    // try
    // {
    // break;
    // }
    // finally
    // {
    // continue;
    // }
    // }
    // }

    private final static String[] ALPHABET = new String[] { "alpha", "beta", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whisky", "yankee", "x-ray", "zebra" };

    public void testBinCursor()
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();

        {
            creator.newBin("recipients");
            depot = creator.create(file);
        }

        Depot.Snapshot snapshot = depot.newSnapshot();
        Depot.Marshaller marshaller = new Depot.SerializationMarshaller();
        for (int i = 0; i < 1024; i++)
        {
            String letter = ALPHABET[i % ALPHABET.length];
            snapshot.getBin("recipients").add(marshaller, new Recipient(letter + "@alphabet.com", letter, letter));
        }

        snapshot.commit();

        snapshot = depot.newSnapshot();

        int i = 0;

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();
        Depot.Bin.Cursor cursor = snapshot.getBin("recipients").first(unmarshaller);
        while (cursor.hasNext())
        {
            Depot.Bag bag = cursor.nextBag();
            Recipient recipient = (Recipient) bag.getObject();
            assertEquals(ALPHABET[i++ % ALPHABET.length], recipient.getFirstName());
        }

        snapshot.rollback();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */