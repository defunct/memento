/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.File;
import java.io.IOException;

import com.agtrz.depot.Depot.Marshaller;

public class UsageTestCase
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

    public void testUsage()
    {
        File file =  newFile();
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
        
        Marshaller marshaller = new Depot.SerialzationMarshaller();
        Depot.Bag person = snapshot.getBin("people").add(marshaller, alan);
        Depot.Bag message = snapshot.getBin("messages").add(marshaller, hello);
        Depot.Bag bounce = snapshot.getBin("bounces").add(marshaller, received);
        
        person.link("messages", new Depot.Bag[] { message, bounce });
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */