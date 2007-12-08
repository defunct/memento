/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import nu.xom.Attribute;
import nu.xom.Builder;
import nu.xom.Comment;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.ParsingException;
import nu.xom.Serializer;
import nu.xom.Text;
import nu.xom.ValidityException;

import org.custommonkey.xmlunit.XMLTestCase;
import org.xml.sax.SAXException;
import org.xmlpull.mxp1.MXParser;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import EDU.oswego.cs.dl.util.concurrent.NullSync;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.xml.PrettyPrintWriter;
import com.thoughtworks.xstream.io.xml.XmlFriendlyReplacer;
import com.thoughtworks.xstream.io.xml.XppReader;

public class UsageTestCase
extends XMLTestCase
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

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();
        Depot.Bag recipient = snapshot.getBin("recipients").add(alan);

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

        final Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();
        Depot.Bag recipient = one.getBin("recipients").add(alan);

        Long keyOfAlan = recipient.getKey();

        assertEquals(alan, recipient.getObject());

        Depot.Snapshot two = depot.newSnapshot();
        recipient = two.getBin("recipients").get(unmarshaller, keyOfAlan);

        assertNull(recipient);

        one.commit();

        recipient = two.getBin("recipients").get(unmarshaller, keyOfAlan);
        assertNull(recipient);

        Depot.Test test = Depot.newTest();
        final Depot.Snapshot three = depot.newSnapshot(test, new NullSync());
        recipient = three.getBin("recipients").add(bart);

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

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();
        Long key = one.getBin("recipients").add(alan).getKey();
        one.commit();

        Thread.sleep(1);
        Depot.Snapshot two = depot.newSnapshot();
        Depot.Bag person = two.getBin("recipients").get(unmarshaller, key);

        assertNotNull(person);
        assertEquals(alan, person.getObject());

        Recipient kiloblog = new Recipient("alan@kiloblog.com", alan.getFirstName(), alan.getLastName());

        Thread.sleep(1);
        Depot.Snapshot three = depot.newSnapshot();
        Depot.Bag updated = three.getBin("recipients").update(key, kiloblog);

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

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Long key = one.getBin("recipients").add(alan).getKey();
        one.commit();

        Depot.Snapshot two = depot.newSnapshot();
        Depot.Bag person = two.getBin("recipients").get(unmarshaller, key);

        assertNotNull(person);
        assertEquals(alan, person.getObject());

        Recipient kiloblog = new Recipient("alan@kiloblog.com", alan.getFirstName(), alan.getLastName());

        Depot.Snapshot three = depot.newSnapshot();
        Depot.Bag updated = three.getBin("recipients").update(key, kiloblog);

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

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Bag person = snapshot.getBin("recipients").add(alan);
        Depot.Bag message = snapshot.getBin("messages").add(hello);
        Depot.Bag bounce = snapshot.getBin("bounces").add(received);

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
            Depot.Index.Creator lastNameFirst = recipients.newIndex("lastNameFirst");
            lastNameFirst.setExtractor(new FieldExtractor());
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

        Depot.Bag person = snapshot.getBin("recipients").add(alan);
        assertEquals(alan, person.getObject());

        person = snapshot.getBin("recipients").add(angelo);
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

        snapshot.getBin("recipients").update(person.getKey(), frank);

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
            Depot.Index.Creator newIndex = recipients.newIndex("lastNameFirst");
            newIndex.setExtractor(new FieldExtractor());
            newIndex.setUnique(true);
            depot = creator.create(file);
        }

        depot.newSnapshot().commit();

        Recipient alan1 = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Recipient alan2 = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Recipient angelo = new Recipient("angelo@thinknola.com", "Angelo", "Silvestri");
        Recipient frank = new Recipient("frank@thinknola.com", "Frank", "Silvestri");
        Recipient frank2 = new Recipient("frank@thinknola.com", "Angelo", "Silvestri");

        Depot.Snapshot snapshot = depot.newSnapshot();
        snapshot.getBin("recipients").add(alan1);
        boolean exceptional = false;
        try
        {
            snapshot.getBin("recipients").add(alan2);
        }
        catch (Depot.Error e)
        {
            assertEquals(Depot.UNIQUE_CONSTRAINT_VIOLATION_ERROR, e.code);
            exceptional = true;
        }
        assertTrue(exceptional);

        snapshot.getBin("recipients").add(frank);
        snapshot.getBin("recipients").add(angelo);
        exceptional = false;
        try
        {
            snapshot.getBin("recipients").add(frank2);
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
            snapshot.getBin("recipients").add(frank2);
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
            Depot.Index.Creator newIndex = recipients.newIndex("lastNameFirst");
            newIndex.setExtractor(new FieldExtractor());
            newIndex.setUnique(true);
            depot = creator.create(file);
        }

        depot.newSnapshot().commit();

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");

        Depot.Snapshot one = depot.newSnapshot();
        one.getBin("recipients").add(alan);

        Depot.Snapshot two = depot.newSnapshot();
        two.getBin("recipients").add(alan);

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
            Depot.Index.Creator newIndex = recipients.newIndex("lastNameFirst");
            newIndex.setExtractor(new FieldExtractor());
            newIndex.setUnique(true);
            depot = creator.create(file);
        }

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");

        Depot.Snapshot one = depot.newSnapshot();
        one.getBin("recipients").add(alan);

        Depot.Test test = Depot.newTest();
        final Depot.Snapshot two = depot.newSnapshot(test, new NullSync());
        two.getBin("recipients").add(alan);

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

        Recipient alan1 = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");

        Depot.Snapshot one = depot.newSnapshot();
        Depot.Bag person = one.getBin("recipients").add(alan1);

        one.commit();

        Recipient alan2 = new Recipient("alan@kiloblog.com", "Alan", "Gutierrez");

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Snapshot two = depot.newSnapshot();
        person = two.getBin("recipients").get(unmarshaller, person.getKey());
        two.getBin("recipients").update(person.getKey(), alan2);

        Depot.Snapshot three = depot.newSnapshot();
        person = three.getBin("recipients").get(unmarshaller, person.getKey());
        three.getBin("recipients").update(person.getKey(), alan2);

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

        Recipient alan1 = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");

        Depot.Snapshot one = depot.newSnapshot();
        Depot.Bag person = one.getBin("recipients").add(alan1);

        one.commit();

        Recipient alan2 = new Recipient("alan@kiloblog.com", "Alan", "Gutierrez");

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Snapshot two = depot.newSnapshot();
        person = two.getBin("recipients").get(unmarshaller, person.getKey());
        two.getBin("recipients").update(person.getKey(), alan2);

        Depot.Test test = Depot.newTest();
        final Depot.Snapshot three = depot.newSnapshot(test, new NullSync());
        person = three.getBin("recipients").get(unmarshaller, person.getKey());
        three.getBin("recipients").update(person.getKey(), alan2);

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

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Bag person = one.getBin("recipients").add(alan);
        Depot.Bag message = one.getBin("messages").add(hello);
        Depot.Bag bounce = one.getBin("bounces").add(received);

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
        final Depot.Snapshot three = depot.newSnapshot(test, new NullSync());

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

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Bag person = one.getBin("recipients").add(alan);
        Depot.Bag message = one.getBin("messages").add(hello);
        Depot.Bag bounce = one.getBin("bounces").add(received);

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
        for (int i = 0; i < 1024; i++)
        {
            String letter = ALPHABET[i % ALPHABET.length];
            snapshot.getBin("recipients").add(new Recipient(letter + "@alphabet.com", letter, letter));
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

    public void testBackup() throws IOException
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();
        {
            Depot.Bin.Creator recipients = creator.newBin("recipients");
            Depot.Bin.Creator messages = creator.newBin("messages");
            Depot.Bin.Creator bounces = creator.newBin("bounces");

            Depot.Index.Creator firstNameLast = recipients.newIndex("firstNameLast");
            firstNameLast.setExtractor(new FieldExtractor());
            firstNameLast.setNotNull(true);
            firstNameLast.setUnique(true);

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

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Depot.Bag person = snapshot.getBin("recipients").add(alan);
        Depot.Bag message = snapshot.getBin("messages").add(hello);
        Depot.Bag bounce = snapshot.getBin("bounces").add(received);

        Map select = new HashMap();

        select.put("recipients", person.getKey());
        select.put("messages", message.getKey());
        select.put("bounces", bounce.getKey());

        Map relink = new HashMap(select);

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

        snapshot.commit();

        snapshot = depot.newSnapshot();

        snapshot.getJoin("messages").link(relink);

        snapshot.commit();

        snapshot = depot.newSnapshot();

        XStream xstream = new XStream();
        // ObjectOutputStream out = xstream.createObjectOutputStream(new
        // PrettyPrintWriter(new OutputStreamWriter(System.out, "UTF-8")),
        // "depot");
        // snapshot.dump(out);
        // out.close();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream out = xstream.createObjectOutputStream(new PrettyPrintWriter(new OutputStreamWriter(bytes, "UTF-8")), "depot");
        snapshot.dump(out);
        out.close();

        snapshot.rollback();

        depot.close();

        ObjectInputStream in = xstream.createObjectInputStream(new XppReader(new InputStreamReader(new ByteArrayInputStream(bytes.toByteArray()), "UTF-8")));

        File recovered = newFile();
        depot = new Depot.Loader().load(in, recovered, new NullSync());

        snapshot = depot.newSnapshot();

        select.put("recipients", person.getKey());
        linked = snapshot.getJoin("messages").find(select);

        assertTrue(linked.hasNext());
        while (linked.hasNext())
        {
            tuple = (Depot.Tuple) linked.next();
            assertEquals(alan, tuple.getBag(unmarshaller, "recipients").getObject());
        }
    }

    public static class ExposedXppReader
    extends XppReader
    {
        private XmlPullParser parser;

        public ExposedXppReader(Reader reader)
        {
            this(reader, new XmlFriendlyReplacer());
        }

        public ExposedXppReader(Reader reader, XmlFriendlyReplacer replacer)
        {
            super(reader, replacer);
        }

        protected XmlPullParser createParser()
        {
            parser = new MXParser();
            try
            {
                parser.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);
            }
            catch (XmlPullParserException e)
            {
                throw new Error(e);
            }
            return parser;
        }

        public XmlPullParser getParser()
        {
            return parser;
        }
    }

    public final static class XOMConverter
    implements Converter
    {
        private final Serializer serializer;

        public XOMConverter(OutputStream out)
        {
            try
            {
                serializer = new Serializer(out, "UTF-8")
                {
                    public void write(Document document) throws IOException
                    {
                        write(document.getRootElement());
                        flush();
                    }
                };
            }
            catch (UnsupportedEncodingException e)
            {
                throw new Error(e);
            }
        }

        public boolean canConvert(Class type)
        {
            return type.equals(Document.class);
        }

        public void marshal(Object object, HierarchicalStreamWriter writer, MarshallingContext context)
        {
            writer.setValue("");
            writer.flush();
            try
            {
                serializer.write((Document) object);
            }
            catch (IOException e)
            {
                new ParsingException("Cannot write XML document.", e);
            }
            writer.flush();
        }

        private Attribute.Type getAttributeType(String type)
        {
            if (type.equals("CDATA"))
            {
                return Attribute.Type.CDATA;
            }
            if (type.equals("ENTITIES"))
            {
                return Attribute.Type.ENTITIES;
            }
            if (type.equals("ENTITY"))
            {
                return Attribute.Type.ENTITY;
            }
            if (type.equals("ENUMERATION"))
            {
                return Attribute.Type.ENUMERATION;
            }
            if (type.equals("ID"))
            {
                return Attribute.Type.ID;
            }
            if (type.equals("IDREF"))
            {
                return Attribute.Type.IDREF;
            }
            if (type.equals("IDREFS"))
            {
                return Attribute.Type.IDREFS;
            }
            if (type.equals("NMTOKEN"))
            {
                return Attribute.Type.NMTOKEN;
            }
            if (type.equals("NMTOKENS"))
            {
                return Attribute.Type.NMTOKENS;
            }
            if (type.equals("NOTATION"))
            {
                return Attribute.Type.NOTATION;
            }
            if (type.equals("UNDECLARED"))
            {
                return Attribute.Type.UNDECLARED;
            }
            throw new IllegalStateException();
        }

        private Element readElement(XmlPullParser parser) throws XmlPullParserException
        {
            Element element = null;
            if (parser.getNamespace().equals(""))
            {
                element = new Element(parser.getName());
            }
            else
            {
                String prefix = parser.getPrefix();
                String name = parser.getName();
                String namespace = parser.getNamespace();
                if (prefix != null)
                {
                    name = prefix + ':' + name;
                }
                element = new Element(name, namespace);
            }
            int start = parser.getNamespaceCount(parser.getDepth() - 1);
            int end = parser.getNamespaceCount(parser.getDepth());
            for (int i = start; i < end; i++)
            {
                String prefix = parser.getNamespacePrefix(i);
                String namespace = parser.getNamespaceUri(i);
                if (prefix != null)
                {
                    element.addNamespaceDeclaration(prefix, namespace);
                }
            }
            end = parser.getAttributeCount();
            for (int i = 0; i < end; i++)
            {
                Attribute attribute = null;
                String name = parser.getAttributeName(i);
                String value = parser.getAttributeValue(i);
                Attribute.Type type = getAttributeType(parser.getAttributeType(i));
                if (parser.getAttributeNamespace(i).equals(""))
                {
                    attribute = new Attribute(name, value, type);
                }
                else
                {
                    String namespace = parser.getAttributeNamespace(i);
                    attribute = new Attribute(namespace, name, value, type);
                }
                element.addAttribute(attribute);
            }
            return element;
        }

        private void next(Element element, XmlPullParser parser) throws XmlPullParserException, IOException
        {
            int type = 0;
            while ((type = parser.nextToken()) != XmlPullParser.END_TAG)
            {
                switch (type)
                {
                    case XmlPullParser.START_TAG:
                        Element child = readElement(parser);
                        element.appendChild(child);
                        next(child, parser);
                        break;
                    case XmlPullParser.TEXT:
                        Text text = new Text(parser.getText());
                        element.appendChild(text);
                        break;
                    case XmlPullParser.COMMENT:
                        Comment comment = new Comment(parser.getText());
                        element.appendChild(comment);
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }

        }

        public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context)
        {
            ExposedXppReader xReader = (ExposedXppReader) reader.underlyingReader();
            XmlPullParser parser = xReader.getParser();
            Document document = null;
            try
            {
                parser.nextTag();
                Element element = readElement(parser);
                document = new Document(element);
                next(element, parser);
            }
            catch (Exception e)
            {
                throw new Error("Cannot parse XML document.", e);
            }
            return document;
        }
    }

    public void saveRestore(String file) throws ValidityException, ParsingException, IOException, ClassNotFoundException, SAXException, ParserConfigurationException
    {
        Builder builder = new Builder();
        Document control = builder.build(getClass().getResourceAsStream(file));

        List list = new ArrayList();
        list.add(control);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        XStream xstream = new XStream();
        xstream.registerConverter(new XOMConverter(bytes));

        ObjectOutputStream out = xstream.createObjectOutputStream(new PrettyPrintWriter(new OutputStreamWriter(bytes, "UTF-8")), "depot");
        out.writeObject(list);
        out.close();

        System.out.println(bytes.toString());

        ObjectInputStream in = xstream.createObjectInputStream(new ExposedXppReader(new InputStreamReader(new ByteArrayInputStream(bytes.toByteArray()), "UTF-8")));
        list = (List) in.readObject();

        Document actual = (Document) list.get(0);

        assertXMLEqual(control.toXML(), actual.toXML());

        new Serializer(System.out, "UTF-8").write(actual);
    }

    public void testXStream() throws UnsupportedEncodingException, IOException, ClassNotFoundException, ValidityException, ParsingException, SAXException, ParserConfigurationException
    {
        saveRestore("document.xml");
        saveRestore("document-ns.xml");
        saveRestore("text.xml");
        saveRestore("child.xml");
        saveRestore("child-text.xml");
        saveRestore("children.xml");
        saveRestore("mixed.xml");
        saveRestore("comment.xml");
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
