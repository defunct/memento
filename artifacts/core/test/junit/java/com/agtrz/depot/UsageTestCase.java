/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.File;
import java.io.IOException;

import EDU.oswego.cs.dl.util.concurrent.Latch;
import EDU.oswego.cs.dl.util.concurrent.Sync;

import junit.framework.TestCase;

public class UsageTestCase
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

    private Depot newDepot(File file)
    {
        Depot.Creator creator = new Depot.Creator();
        Depot.BinCreator recipients = creator.newBin("recipients");
        Depot.BinCreator messages = creator.newBin("messages");
        Depot.BinCreator bounces = creator.newBin("bounces");

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

        Depot.Marshaller marshaller = new Depot.SerialzationMarshaller();
        Depot.Bag recipient = snapshot.getBin("recipients").add(marshaller, alan);

        Long key = recipient.getKey();

        assertEquals(alan, recipient.getObject());

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();
        recipient = snapshot.getBin("recipients").get(unmarshaller, key);

        assertNotNull(recipient);
        assertEquals(alan, recipient.getObject());

        snapshot.commit();

        snapshot = depot.newSnapshot();

        recipient = snapshot.getBin("recipients").get(unmarshaller, key);

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

        Depot.Marshaller marshaller = new Depot.SerialzationMarshaller();
        Depot.Bag recipient = one.getBin("recipients").add(marshaller, alan);

        Long keyOfAlan = recipient.getKey();

        assertEquals(alan, recipient.getObject());

        Depot.Unmarshaller unmarshaller = new Depot.SerializationUnmarshaller();

        Thread.sleep(1);

        Depot.Snapshot two = depot.newSnapshot();
        recipient = two.getBin("recipients").get(unmarshaller, keyOfAlan);

        assertNull(recipient);

        one.commit();

        recipient = two.getBin("recipients").get(unmarshaller, keyOfAlan);
        assertNull(recipient);

        Thread.sleep(1);

        Depot.Test test = new Depot.Test();
        Sync changesWritten = new Latch();
        Sync registerMutation = new Latch();
        Latch committed = new Latch();
        test.setJournalLatches(changesWritten, registerMutation);
        test.setJournalComplete(committed);
        final Depot.Snapshot three = depot.newSnapshot(test);
        recipient = three.getBin("recipients").add(marshaller, bart);

        Long keyOfBart = recipient.getKey();

        assertEquals(bart, recipient.getObject());

        Thread.sleep(1);
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

    public void testUpdateRecord()
    {

    }

    public void testDeleteRecord()
    {

    }

    public void testUsage()
    {
        File file = newFile();
        Depot depot = null;
        Depot.Creator creator = new Depot.Creator();
        {
            Depot.BinCreator recipients = creator.newBin("recipients");
            Depot.BinCreator messages = creator.newBin("messages");
            Depot.BinCreator bounces = creator.newBin("bounces");

            recipients.newJoin("messages").add(messages).add(bounces);
            messages.newJoin("recipients").add(recipients).add(bounces);

            depot = creator.create(file);
        }

        Recipient alan = new Recipient("alan@blogometer.com", "Alan", "Gutierrez");
        Message hello = new Message();
        Bounce received = new Bounce();

        Depot.Snapshot snapshot = depot.newSnapshot();

        Depot.Marshaller marshaller = new Depot.SerialzationMarshaller();
        Depot.Bag person = snapshot.getBin("people").add(marshaller, alan);
        Depot.Bag message = snapshot.getBin("messages").add(marshaller, hello);
        Depot.Bag bounce = snapshot.getBin("bounces").add(marshaller, received);

        person.link("messages", new Depot.Bag[] { message, bounce });
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */