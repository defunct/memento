package com.agtrz.depot;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;

import com.goodworkalan.memento.Danger;
import com.goodworkalan.memento.Restoration;
import com.goodworkalan.memento.Snapshot;
import com.goodworkalan.memento.Sync;


public final class Loader
{
    public Depot load(ObjectInputStream in, File file, Sync sync)
    {
        Restoration.Schema schema;
        try
        {
            schema = (Restoration.Schema) in.readObject();
        }
        catch (IOException e)
        {
            throw new Danger("io", e, 400);
        }
        catch (ClassNotFoundException e)
        {
            throw new Danger("class.not.found", e, 400);
        }
        Depot depot = schema.newDepot(file, sync);
        Snapshot snapshot = depot.newSnapshot();
        for (;;)
        {
            Restoration.Insert insert;
            try
            {
                insert = (Restoration.Insert) in.readObject();
            }
            catch (EOFException e)
            {
                break;
            }
            catch (IOException e)
            {
                throw new Danger("io", e, 400);
            }
            catch (ClassNotFoundException e)
            {
                throw new Danger("class.not.found", e, 400);
            }
            if (insert == null)
            {
                break;
            }
            insert.insert(snapshot);
        }
        snapshot.commit();
        return depot;
    }
}