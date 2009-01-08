/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.goodworkalan.memento.Bin;
import com.goodworkalan.memento.Index;
import com.goodworkalan.memento.Indexer;
import com.goodworkalan.memento.Janitor;
import com.goodworkalan.memento.Join;
import com.goodworkalan.memento.Latch;
import com.goodworkalan.memento.NullSync;
import com.goodworkalan.memento.Snapshot;
import com.goodworkalan.memento.Sync;
import com.goodworkalan.pack.Pack;

public class Depot
{
    final static int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;
    final static int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
    final static int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
    
    public final static int CORRUPT_FILE_EXCEPTION = 501;

    public final static int CONCURRENT_MODIFICATION_ERROR = 301;

    public final static int UNIQUE_CONSTRAINT_VIOLATION_ERROR = 302;

    public final static int NOT_NULL_VIOLATION_ERROR = 303;

    final static URI HEADER_URI = URI.create("http://syndibase.agtrz.com/strata");

    private final static Integer OPERATING = new Integer(1);

    final static Integer COMMITTED = new Integer(2);

    private final Pack pack;

    private final Schema schema;
    
    private final Strata snapshots;

    private final Map<String, Bin.Common> mapOfBinCommons;

    private final Map<String, Bin.Schema> mapOfBinSchemas;

    final Map<String, Join.Schema> mapOfJoinSchemas;

    private final Sync sync;

    public Depot(File file, Pack pack, Schema schema, Strata mutations, Map<String, Bin.Common> mapOfBinCommons, Map<String, Bin.Schema> mapOfBinSchemas, Map<String, Join.Schema> mapOfJoinSchemas, Sync sync)
    {
        this.mapOfBinCommons = mapOfBinCommons;
        this.mapOfBinSchemas = mapOfBinSchemas;
        this.mapOfJoinSchemas = mapOfJoinSchemas;
        this.snapshots = mutations;
        this.pack = pack;
        this.schema = schema;
        this.sync = sync;
    }

    public Sync getSync()
    {
        return sync;
    }

    @SuppressWarnings("unchecked")
    private static int compare(Comparable<?> left, Comparable<?> right)
    {
        return ((Comparable) left).compareTo(right);
    }

    static boolean partial(Comparable<?>[] partial, Comparable<?>[] full)
    {
        for (int i = 0; i < partial.length; i++)
        {
            if (partial[i] == null)
            {
                if (full[i] != null)
                {
                    return false;
                }
            }
            else if (full[i] == null)
            {
                return false;
            }
            else if (compare(partial[i], full[i]) != 0)
            {
                return false;
            }
        }
        return true;
    }

    static int compare(Comparable<?>[] left, Comparable<?>[] right)
    {
        for (int i = 0; i < left.length; i++)
        {
            int compare = compare(left[i], right[i]);
            if (compare != 0)
            {
                return compare;
            }
        }
        return 0;
    }

    public static boolean hasNulls(Comparable<?>[] fields)
    {
        for (int i = 0; i < fields.length; i++)
        {
            if (fields[i] == null)
            {
                return true;
            }
        }
        return false;
    }

    // FIXME Wait for all processes to end.
    public void close()
    {
        pack.close();
    }

    public <T> void createIndex(Indexer<T> indexer)
    {
        
    }

    public synchronized Snapshot newSnapshot(Test test, Sync sync)
    {
        Long version = new Long(System.currentTimeMillis());
        Snapshot.Record record = new Snapshot.Record(version, OPERATING);
        Pack.Mutator mutator = pack.mutate();

        Strata.Query query = snapshots.query(Fossil.txn(mutator));

        Set<Long> setOfCommitted = new TreeSet<Long>();
        Strata.Cursor versions = query.first();
        while (versions.hasNext())
        {
            Snapshot.Record mutation = (com.goodworkalan.memento.Record) versions.next();
            if (mutation.state.equals(COMMITTED))
            {
                setOfCommitted.add(mutation.version);
            }
        }
        versions.release();

        query.insert(record);

        mutator.commit();

        return new Snapshot(snapshots, schema, mapOfBinCommons, mapOfBinSchemas, mapOfJoinSchemas, mutator, setOfCommitted, test, version, sync);
    }

    public Snapshot newSnapshot(Sync sync)
    {
        return newSnapshot(new Test(new NullSync(), new NullSync(), new NullSync()), sync);
    }

    public Snapshot newSnapshot()
    {
        sync.acquire();
        return newSnapshot(new Test(new NullSync(), new NullSync(), new NullSync()), sync);
    }

    public Iterator<String> getBinNames()
    {
        return Collections.unmodifiableSet(mapOfBinCommons.keySet()).iterator();
    }

    public Iterator<String> getJoinNames()
    {
        return Collections.unmodifiableSet(mapOfJoinSchemas.keySet()).iterator();
    }

    private final static int SWAG_RECORD_LENGTH = 3 * SIZEOF_LONG;
    
 
    private static Strata newJoinStrata(Pack.Mutator mutator, int size)
    {
        Fossil.Schema newJoinStorage = new Fossil.Schema();
        newJoinStorage.setWriter(new Join.Writer(size));
        newJoinStorage.setReader(new Join.Reader(size));
        newJoinStorage.setSize(SIZEOF_LONG * size + SIZEOF_LONG + SIZEOF_SHORT);

        Strata.Schema newJoinStrata = new Strata.Schema();

        newJoinStrata.setStorage(newJoinStorage);
        newJoinStrata.setFieldExtractor(new Join.Extractor());
        newJoinStrata.setSize(180);
        newJoinStrata.setMaxDirtyTiers(1);

        return newJoinStrata.newStrata(Fossil.txn(mutator));
    }

    private static Join.Index newJoinIndex(Pack.Mutator mutator, Map<String, String> mapOfFields, String[] order)
    {
        List<String> listOfFields = new ArrayList<String>();
        for (int i = 0; i < order.length; i++)
        {
            listOfFields.add(order[i]);
        }
        if (order.length < mapOfFields.size())
        {
            Iterator<String> fields = mapOfFields.keySet().iterator();
            while (fields.hasNext())
            {
                String field = (String) fields.next();
                if (!listOfFields.contains(field))
                {
                    listOfFields.add(field);
                }
            }
        }
        String[] fields = (String[]) listOfFields.toArray(new String[listOfFields.size()]);
        return new Join.Index(newJoinStrata(mutator, fields.length), fields);
    }

    private static Map<String, Bin.Common> newMapOfBinCommons(Map<String, Bin.Schema> mapOfBinSchemas, Pack.Mutator mutator)
    {
        Map<String, Bin.Common> mapOfBinCommons = new HashMap<String, Bin.Common>();
        Iterator<Map.Entry<String, Bin.Schema>> bins = mapOfBinSchemas.entrySet().iterator();
        while (bins.hasNext())
        {
            Map.Entry<String, Bin.Schema> entry = bins.next();
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

        return mapOfBinCommons;
    }

    public final static Test newTest()
    {
        return new Test(new Latch(), new Latch(), new Latch());
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */