package com.goodworkalan.memento;

import static com.agtrz.depot.Depot.COMMITTED;
import static com.agtrz.depot.Depot.HEADER_URI;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.goodworkalan.pack.Pack;

public final class Opener
{
    public Depot open(File file)
    {
        return open(file, new NullSync());
    }

    public Depot open(File file, Sync sync)
    {
        Pack.Opener opener = new Pack.Opener();
        Pack pack = opener.open(file);
        Pack.Mutator mutator = pack.mutate();
        ByteBuffer block = mutator.read(mutator.getSchema().getStaticPageAddress(Depot.HEADER_URI));
        long addressOfBags = block.getLong();
        Strata mutations = null;
        Map<String, Bin.Schema> mapOfBinSchemas = null;
        Map<String, Join.Schema> mapOfJoinSchemas = null;
        try
        {
            ObjectInputStream objects = new ObjectInputStream(new ByteBufferInputStream(mutator.read(addressOfBags)));
            mutations = (Strata) objects.readObject();
            mapOfBinSchemas = toBinSchemaMap(objects.readObject());
            mapOfJoinSchemas = toJoinSchemaMap(objects.readObject());
        }
        catch (IOException e)
        {
            throw new Danger("io", 0);
        }
        catch (ClassNotFoundException e)
        {
            throw new Danger("io", 0);
        }
        mutator.commit();

        Map<String, Bin.Common> mapOfBinCommons = new HashMap<String, Bin.Common>();
        for (Map.Entry<String, Bin.Schema> entry : mapOfBinSchemas.entrySet())
        {
            Bin.Schema binSchema = (com.goodworkalan.memento.Schema) entry.getValue();

            long identifer = 1L;
            Strata.Query query = binSchema.getStrata().query(Fossil.txn(mutator));
            Strata.Cursor last = query.first();
            // FIXME You can use hasPrevious when it is implemented.
            while (last.hasNext())
            {
                Bin.Record record = (com.goodworkalan.memento.Record) last.next();
                identifer = record.key + 1;
            }
            last.release();

            mapOfBinCommons.put(entry.getKey(), new Bin.Common(identifer));
        }
        Strata.Query query = mutations.query(Fossil.txn(mutator));

        Set<Long> setOfCommitted = new TreeSet<Long>();
        Strata.Cursor versions = query.first();
        while (versions.hasNext())
        {
            Snapshot.Record mutation = (com.goodworkalan.memento.Record) versions.next();
            if (mutation.state.equals(Depot.COMMITTED))
            {
                setOfCommitted.add(mutation.version);
            }
        }
        versions.release();

        Snapshot snapshot = new Snapshot(mutations, new Schema(pack), mapOfBinCommons, mapOfBinSchemas, mapOfJoinSchemas, pack.mutate(), setOfCommitted, new Test(new NullSync(), new NullSync(), new NullSync()), new Long(0L), new NullSync());
        for (long address : opener.getTemporaryBlocks())
        {
            mutator = pack.mutate();
            block = mutator.read(address);
            Janitor janitor = null;
            try
            {
                ObjectInputStream in = new ObjectInputStream(new ByteBufferInputStream(block));
                janitor = (Janitor) in.readObject();
            }
            catch (Exception e)
            {
                throw new Danger("Cannot reopen journal.", e, 0);
            }
            janitor.rollback(snapshot);
            mutator.commit();

            mutator.free(address);
            janitor.dispose(mutator, true);
            mutator.commit();
        }

        return new Depot(file, pack, new Schema(pack), mutations, mapOfBinCommons, mapOfBinSchemas, mapOfJoinSchemas, sync);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Join.Schema> toJoinSchemaMap(Object object)
    {
        return (Map<String, Join.Schema>) object;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Bin.Schema> toBinSchemaMap(Object object)
    {
        return (Map<String, Bin.Schema>) object;
    }
}