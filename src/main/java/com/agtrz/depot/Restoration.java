package com.agtrz.depot;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.goodworkalan.memento.JoinSchema;
import com.goodworkalan.memento.Snapshot;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;

public final class Restoration
{
    public final class Schema
    implements Serializable
    {
        private static final long serialVersionUID = 20071025L;

        private final Map<String, BinSchema> mapOfBinSchemas;

        private final Map<String, JoinSchema> mapOfJoinSchemas;

        public Schema(Map<String, BinSchema> mapOfBinSchemas, Map<String, JoinSchema> mapOfJoinSchemas)
        {
            this.mapOfBinSchemas = new HashMap<String, BinSchema>(mapOfBinSchemas);
            this.mapOfJoinSchemas = new HashMap<String, JoinSchema>(mapOfJoinSchemas);

            Iterator<String> bins = new HashSet<String>(this.mapOfBinSchemas.keySet()).iterator();
            while (bins.hasNext())
            {
                String name = (String) bins.next();
                BinSchema schema = (BinSchema) this.mapOfBinSchemas.get(name);
                this.mapOfBinSchemas.put(name, schema.toSchema());
            }
            Iterator<String> joins = new HashSet<String>(this.mapOfJoinSchemas.keySet()).iterator();
            while (joins.hasNext())
            {
                String name = (String) joins.next();
                JoinSchema schema = (JoinSchema) this.mapOfJoinSchemas.get(name);
                this.mapOfJoinSchemas.put(name, schema.toSchema());
            }
        }

        public Depot newDepot(File file, Sync sync)
        {
            EmptyDepot empty = new EmptyDepot(file);

            Mutator mutator = empty.pack.mutate();
            Object txn = Fossil.txn(mutator);

            Iterator<String> bins = new HashSet<String>(mapOfBinSchemas.keySet()).iterator();
            while (bins.hasNext())
            {
                String name = (String) bins.next();
                BinSchema schema = mapOfBinSchemas.get(name);
                mapOfBinSchemas.put(name, schema.toStrata(txn));
            }

            Iterator<String> joins = new HashSet<String>(Restoration.this.depot.mapOfJoinSchemas.keySet()).iterator();
            while (joins.hasNext())
            {
                String name = (String) joins.next();
                JoinSchema schema = Restoration.this.depot.mapOfJoinSchemas.get(name);
                Restoration.this.depot.mapOfJoinSchemas.put(name, schema.toStrata(txn));
            }

            Map<String, BinCommon> mapOfBinCommons = newMapOfBinCommons(mapOfBinSchemas, mutator);
            return new Depot(empty.file, empty.pack, new DepotSchema(empty.pack), empty.snapshots, mapOfBinCommons, mapOfBinSchemas, Restoration.this.depot.mapOfJoinSchemas, sync);
        }
    }

    public interface Insert
    {
        public void insert(Snapshot snapshot);
    }

    public final static class Bag
    implements Insert, Serializable
    {
        private static final long serialVersionUID = 20071025L;

        private final String name;

        private final Long key;

        private final Object object;

        public Bag(String name, Long key, Object object)
        {
            this.name = name;
            this.key = key;
            this.object = object;
        }

        public String getName()
        {
            return name;
        }

        public Long getKey()
        {
            return key;
        }

        public Object getObject()
        {
            return object;
        }

        public void insert(Snapshot snapshot)
        {
            snapshot.getBin(name).restore(key, object);
        }
    }

    public final static class Join
    implements Insert, Serializable
    {
        private static final long serialVersionUID = 20071025L;

        private final String name;

        private final Map<String, Long> mapOfKeys;

        public Join(String name, Map<String, Long> mapOfKeys)
        {
            this.name = name;
            this.mapOfKeys = mapOfKeys;
        }

        public void insert(Snapshot snapshot)
        {
            snapshot.getJoin(name).link(mapOfKeys);
        }
    }
}