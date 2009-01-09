package com.goodworkalan.memento;

import static com.goodworkalan.memento.Depot.CONCURRENT_MODIFICATION_ERROR;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;

public final class Join
{
    private final Snapshot snapshot;

    private final JoinSchema schema;

    private final Mutator mutator;

    private final Strata.Query[] isolation;

    public Join(Snapshot snapshot, Pack.Mutator mutator, Join.Schema schema, String name, Map<Long, Join.Janitor> mapOfJanitors)
    {
        Strata[] isolations = new Strata[schema.indices.length];

        for (int i = 0; i < schema.indices.length; i++)
        {
            Strata.Schema creator = new Strata.Schema();

            creator.setFieldExtractor(new Extractor());
            creator.setMaxDirtyTiers(5);
            creator.setSize(180);

            Fossil.Schema newStorage = new Fossil.Schema();

            newStorage.setReader(new Reader(schema.mapOfFields.size()));
            newStorage.setWriter(new Writer(schema.mapOfFields.size()));
            newStorage.setSize(Depot.SIZEOF_LONG * schema.mapOfFields.size() + Depot.SIZEOF_LONG + Depot.SIZEOF_SHORT);

            creator.setStorage(newStorage);

            isolations[i] = creator.newStrata(Fossil.txn(mutator));
        }

        Join.Janitor janitor = new Janitor(isolations, name);

        PackOutputStream allocation = new PackOutputStream(mutator);
        try
        {
            ObjectOutputStream out = new ObjectOutputStream(allocation);
            out.writeObject(janitor);
        }
        catch (IOException e)
        {
            throw new Danger("Cannot write output stream.", e, 0);
        }

        mapOfJanitors.put(allocation.temporary(), janitor);

        Strata.Query[] isolationQueries = new Strata.Query[isolations.length];
        for (int i = 0; i < isolations.length; i++)
        {
            isolationQueries[i] = isolations[i].query(Fossil.txn(mutator));
        }

        this.snapshot = snapshot;
        this.isolation = isolationQueries;
        this.schema = schema;
        this.mutator = mutator;
    }

    void flush()
    {
        for (int i = 0; i < isolation.length; i++)
        {
            isolation[i].flush();
        }
    }

    public void unlink(Map<String, Long> mapOfKeys)
    {
        add(mapOfKeys, snapshot.getVersion(), true);
    }

    public void link(Map<String, Long> mapOfKeys)
    {
        add(mapOfKeys, snapshot.getVersion(), false);
    }

    private void insertIsolation(Map<String, Long> mapOfKeys, Long version, boolean deleted, int index)
    {
        Long[] keys = new Long[schema.mapOfFields.size()];
        for (int i = 0; i < keys.length; i++)
        {
            keys[i] = (Long) mapOfKeys.get(schema.indices[index].fields[i]);
        }
        isolation[index].insert(new Record(keys, version, deleted));
    }

    private void removeIsolation(Map<String, Long> mapOfKeys, Long version, boolean deleted, int index)
    {
        Long[] keys = new Long[schema.mapOfFields.size()];
        for (int i = 0; i < keys.length; i++)
        {
            keys[i] = (Long) mapOfKeys.get(schema.indices[index].fields[i]);
        }
        final Join.Record record = new Record(keys, version, deleted);
        isolation[index].remove(keys, new Strata.Deletable()
        {
            public boolean deletable(Object object)
            {
                return record.equals((Record) object);
            }
        });
    }

    private void add(Map<String, Long> mapOfKeys, Long version, boolean deleted)
    {
        Long[] keys = new Long[schema.mapOfFields.size()];
        for (int i = 0; i < keys.length; i++)
        {
            keys[i] = (Long) mapOfKeys.get(schema.indices[0].fields[i]);
        }
        Strata.Cursor cursor = isolation[0].find(keys);
        if (cursor.hasNext())
        {
            final Join.Record record = (com.goodworkalan.memento.Record) cursor.next();
            cursor.release();
            if (Depot.compare(record.keys, keys) == 0)
            {
                for (int i = 0; i < schema.indices.length; i++)
                {
                    removeIsolation(mapOfKeys, version, deleted, i);
                }
            }
        }
        else
        {
            cursor.release();
        }
        for (int i = 0; i < schema.indices.length; i++)
        {
            insertIsolation(mapOfKeys, version, deleted, i);
        }
    }

    public Join.Cursor find(Map<String, Long> mapOfKeys)
    {
        // if (mapOfKeys.size() == 0)
        // {
        // throw new IllegalArgumentException();
        // }
        Iterator<String> fields = mapOfKeys.keySet().iterator();
        while (fields.hasNext())
        {
            if (!schema.mapOfFields.containsKey(fields.next()))
            {
                throw new IllegalArgumentException();
            }
        }
        int index = 0;
        int most = 0;
        for (int i = 0; i < schema.indices.length; i++)
        {
            int count = 0;
            for (int j = 0; j < schema.indices[i].fields.length; j++)
            {
                if (mapOfKeys.containsKey(schema.indices[i].fields[j]))
                {
                    count++;
                }
                else
                {
                    break;
                }
            }
            if (count > most)
            {
                most = count;
                index = i;
            }
        }

        Strata.Query common = schema.indices[index].getStrata().query(Fossil.txn(mutator));
        Long[] keys = new Long[most];
        if (most == 0)
        {
            return new Cursor(snapshot, keys, mapOfKeys, common.first(), isolation[index].first(), schema, schema.indices[index]);
        }

        Map<String, Long> mapToScan = new HashMap<String, Long>(mapOfKeys);
        for (int i = 0; i < most; i++)
        {
            String field = schema.indices[index].fields[i];
            keys[i] = (Long) mapOfKeys.get(field);
            mapToScan.remove(field);
        }

        return new Cursor(snapshot, keys, mapToScan, common.find(keys), isolation[index].find(keys), schema, schema.indices[index]);
    }

    // FIXME Call this somewhere somehow.
    void copacetic()
    {
        for (int i = 0; i < schema.indices.length; i++)
        {
            Set<Object> seen = new HashSet<Object>();
            Strata.Cursor isolated = isolation[i].first();
            while (isolated.hasNext())
            {
                Join.Record record = (com.goodworkalan.memento.Record) isolated.next();
                List<Object> listOfKeys = new ArrayList<Object>();
                for (int j = 0; j < record.keys.length; j++)
                {
                    listOfKeys.add(record.keys[j]);
                }
                if (seen.contains(listOfKeys))
                {
                    throw new Danger("Duplicate key in isolation.", 0);
                }
                seen.add(listOfKeys);
            }
            isolated.release();
        }
    }

    void commit()
    {
        for (int i = 0; i < schema.indices.length; i++)
        {
            Strata.Query query = schema.indices[i].getStrata().query(Fossil.txn(mutator));
            Strata.Cursor isolated = isolation[i].first();
            while (isolated.hasNext())
            {
                query.insert((com.goodworkalan.memento.Record) isolated.next());
            }
            isolated.release();
            query.flush();

            boolean copacetic = true;
            isolated = isolation[i].first();
            while (copacetic && isolated.hasNext())
            {
                Join.Record record = (com.goodworkalan.memento.Record) isolated.next();
                Strata.Cursor cursor = query.find(record.keys);
                while (copacetic && cursor.hasNext())
                {
                    Join.Record candidate = (com.goodworkalan.memento.Record) cursor.next();
                    if (Depot.compare(candidate.keys, record.keys) != 0)
                    {
                        break;
                    }
                    else if (candidate.version.equals(record.version))
                    {
                        break;
                    }
                    else if (!snapshot.isVisible(candidate.version))
                    {
                        copacetic = false;
                    }
                }
                cursor.release();
            }

            if (!copacetic)
            {
                throw new Error("Concurrent modification.", Depot.CONCURRENT_MODIFICATION_ERROR);
            }
        }
    }

    public void vacuum()
    {
        for (int i = 0; i < schema.indices.length; i++)
        {
            Strata.Query query = schema.indices[i].getStrata().query(Fossil.txn(mutator));
            Strata.Cursor cursor = query.first();
            Join.Record previous = null;
            while (cursor.hasNext() && previous == null)
            {
                Join.Record record = (com.goodworkalan.memento.Record) cursor.next();
                if (snapshot.isVisible(record.version))
                {
                    previous = record;
                }
            }
            cursor.release();
            for (;;)
            {
                cursor = query.find(previous);
                Join.Record found = null;
                while (cursor.hasNext() && found == null)
                {
                    Join.Record record = (com.goodworkalan.memento.Record) cursor.next();
                    if (snapshot.isVisible(record.version))
                    {
                        found = record;
                    }
                }
                if (!previous.equals(found))
                {
                    previous = found;
                    cursor.release();
                    continue;
                }
                Join.Record next = null;
                while (cursor.hasNext() && next == null)
                {
                    Join.Record record = (com.goodworkalan.memento.Record) cursor.next();
                    if (snapshot.isVisible(record.version))
                    {
                        next = record;
                    }
                }
                cursor.release();
                if (Depot.compare(previous.keys, next.keys) == 0 || previous.deleted)
                {
                    query.remove(previous);
                    query.flush();
                }
                previous = next;
            }
        }
    }


 

 
}