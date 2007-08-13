/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import EDU.oswego.cs.dl.util.concurrent.NullSync;
import EDU.oswego.cs.dl.util.concurrent.Sync;

import com.agtrz.bento.Bento;
import com.agtrz.strata.ArrayListStorage;
import com.agtrz.strata.Strata;
import com.agtrz.strata.bento.BentoStorage;
import com.agtrz.swag.danger.AsinineCheckedExceptionThatIsEntirelyImpossible;
import com.agtrz.swag.io.ByteBufferInputStream;
import com.agtrz.swag.io.SizeOf;

public class Depot
{
    private final static URI HEADER_URI = URI.create("http://syndibase.agtrz.com/strata");

    private final static Integer OPERATING = new Integer(1);

    private final static Integer COMMITTED = new Integer(2);

    private final Bento bento;

    private final Strata snapshots;

    private final Map mapOfBinCommons;

    public Depot(File file, Bento bento, Strata mutations, Map mapOfBinCommons)
    {
        this.mapOfBinCommons = mapOfBinCommons;
        this.snapshots = mutations;
        this.bento = bento;
    }

    private static boolean partial(Comparable[] partial, Comparable[] full)
    {
        for (int i = 0; i < partial.length; i++)
        {
            if (!partial[i].equals(full[i]))
            {
                return false;
            }
        }
        return true;
    }

    private static int compare(Comparable[] left, Comparable[] right)
    {
        for (int i = 0; i < left.length; i++)
        {
            int compare = left[i].compareTo(right[i]);
            if (compare != 0)
            {
                return compare;
            }
        }
        return 0;
    }

    // FIXME Wait for all processes to end.
    public void close()
    {
        bento.close();
    }

    public synchronized Snapshot newSnapshot(Test test)
    {
        Long version = new Long(System.currentTimeMillis());
        Snapshot.Record record = new Snapshot.Record(version, OPERATING);
        Bento.Mutator mutator = bento.mutate();

        Strata.Query query = snapshots.query(BentoStorage.txn(mutator));

        Set setOfCommitted = new TreeSet();
        Strata.Cursor versions = query.first();
        while (versions.hasNext())
        {
            Snapshot.Record mutation = (Snapshot.Record) versions.next();
            if (mutation.state.equals(COMMITTED))
            {
                setOfCommitted.add(mutation.version);
            }
        }
        versions.release();

        query.insert(record);

        mutator.getJournal().commit();

        mutator = bento.mutate();

        return new Snapshot(snapshots, mapOfBinCommons, mutator, setOfCommitted, test, version);
    }

    public Snapshot newSnapshot()
    {
        return newSnapshot(new Test());
    }

    public final static class Exception
    extends RuntimeException
    {
        private static final long serialVersionUID = 20070210L;

        public final int code;

        public final List listOfParameters;

        public Exception(String message, Throwable cause, int code)
        {
            super(message, cause);
            this.code = code;
            this.listOfParameters = new ArrayList();
        }

        public Exception(String message, int code)
        {
            super(message);
            this.code = code;
            this.listOfParameters = new ArrayList();
        }

        public Exception add(Object parameter)
        {
            listOfParameters.add(parameter);
            return this;
        }
    }

    public static class Bag
    implements Serializable
    {
        private static final long serialVersionUID = 20070210L;

        private final Bin bin;

        private final Long key;

        private final Long version;

        private final Object object;

        public Bag(Bin bin, Long key, Long version, Object object)
        {
            this.bin = bin;
            this.key = key;
            this.version = version;
            this.object = object;
        }

        public Long getKey()
        {
            return key;
        }

        public Object getObject()
        {
            return object;
        }

        public Long getVersion()
        {
            return version;
        }

        public void link(String name, Bag[] bags)
        {
            Long[] keys = new Long[bags.length + 1];

            keys[0] = getKey();

            for (int i = 0; i < bags.length; i++)
            {
                keys[i + 1] = bags[i].getKey();
            }

            bin.getJoin(name).link(keys);
        }

        public void unlink(String name, Bag[] bags)
        {
            Long[] keys = new Long[bags.length + 1];

            keys[0] = getKey();

            for (int i = 0; i < bags.length; i++)
            {
                keys[i + 1] = bags[i].getKey();
            }

            bin.getJoin(name).unlink(keys);
        }

        public Iterator getLinked(String name)
        {
            Join join = bin.getJoin(name);
            return join.find(new Bag[] { this });
        }
    }

    public final static class Bin
    {
        private final String name;

        private final Snapshot snapshot;

        private final Common common;

        private final Map mapOfJoins;

        private final Map mapOfIndices;

        private final Strata.Query query;

        private final Strata.Query isolation;

        public final Map mapOfObjects = new LinkedHashMap();

        int allocations;

        public Bin(Snapshot snapshot, String name, Common common)
        {
            this.snapshot = snapshot;
            this.name = name;
            this.common = common;
            this.mapOfJoins = new HashMap();
            this.mapOfIndices = newIndexMap(snapshot, common);
            this.query = common.schema.strata.query(snapshot);
            this.isolation = newIsolation();
        }

        private static Strata.Query newIsolation()
        {
            Strata.Creator creator = new Strata.Creator();

            creator.setCacheFields(true);
            creator.setFieldExtractor(new Extractor());
            creator.setStorage(new ArrayListStorage());

            return creator.create(null).query(null);
        }

        private static Map newIndexMap(Snapshot snapshot, Common common)
        {
            Map mapOfIndices = new HashMap();
            Iterator entries = common.schema.mapOfIndexSchemas.entrySet().iterator();
            while (entries.hasNext())
            {
                Map.Entry entry = (Map.Entry) entries.next();
                Index.Schema schema = (Index.Schema) entry.getValue();
                mapOfIndices.put(entry.getKey(), new Index(schema));
            }
            return mapOfIndices;
        }

        public String getName()
        {
            return name;
        }

        public Bag add(Marshaller marshaller, Unmarshaller unmarshaller, Object object)
        {
            Bag bag = new Bag(this, common.nextIdentifier(), snapshot.getVersion(), object);
            Bento.OutputStream allocation = new Bento.OutputStream(snapshot.getMutator());
            marshaller.marshall(allocation, object);
            Bento.Address address = allocation.allocate(false);
            isolation.insert(new Record(bag.getKey(), bag.getVersion(), address));
            allocations++;

            Iterator indices = mapOfIndices.values().iterator();
            while (indices.hasNext())
            {
                Index index = (Index) indices.next();
                index.add(this, snapshot.getMutator(), unmarshaller, bag);
            }

            return bag;
        }

        private static boolean isDeleted(Record record)
        {
            return record.address.equals(Bento.NULL_ADDRESS);
        }

        private Record update(Comparable[] key)
        {
            Record record = (Record) isolation.remove(key, Strata.ANY);
            if (record != null)
            {
                Bento.Mutator mutator = snapshot.getMutator();
                mutator.free(mutator.load(record.address));
            }
            else
            {
                record = get(query.find(key), key, false);
            }
            if (record != null)
            {
                record = isDeleted(record) ? null : record;
            }
            return record;
        }

        public Bag update(Marshaller marshaller, Unmarshaller unmarshaller, Long key, Object object)
        {
            Record record = update(new Comparable[] { key });

            if (record == null)
            {
                throw new Exception("update.bag.does.not.exist", 401);
            }

            // Conrad Abadie.
            Bag bag = new Bag(this, key, snapshot.getVersion(), object);
            Bento.OutputStream allocation = new Bento.OutputStream(snapshot.mutator);
            marshaller.marshall(allocation, object);
            Bento.Address address = allocation.allocate(false);
            isolation.insert(new Record(bag.getKey(), bag.getVersion(), address));
            allocations++;

            Iterator indices = mapOfIndices.values().iterator();
            while (indices.hasNext())
            {
                Index index = (Index) indices.next();
                index.update(this, snapshot.getMutator(), unmarshaller, key, record.version, snapshot.getVersion());
            }

            return bag;
        }

        public void delete(Long key)
        {
            Record record = update(new Comparable[] { key });

            if (record == null)
            {
                throw new Exception("delete.bag.does.not.exist", 402);
            }

            // FIXME This is not necessary if the record was inserted in this
            // query.
            isolation.insert(new Record(key, snapshot.getVersion(), Bento.NULL_ADDRESS));
        }

        private Record get(Strata.Cursor cursor, Comparable[] key, boolean isolated)
        {
            Record candidate = null;
            for (;;)
            {
                if (!cursor.hasNext())
                {
                    break;
                }
                Record record = (Record) cursor.next();
                if (!partial(key, new Comparable[] { record.key, record.version }))
                {
                    break;
                }
                if (isolated || snapshot.isVisible(record.version))
                {
                    candidate = record;
                }
            }
            cursor.release();
            return candidate;
        }

        private Bag unmarshall(Unmarshaller unmarshaller, Record record)
        {
            Bento.Block block = snapshot.getMutator().load(record.address);
            Object object = unmarshaller.unmarshall(new ByteBufferInputStream(block.toByteBuffer(), false));
            return new Bag(this, record.key, record.version, object);
        }

        private Record get(Comparable[] key)
        {
            Record stored = get(query.find(key), key, false);
            Record isolated = get(isolation.find(key), key, true);
            if (isolated != null)
            {
                return isDeleted(isolated) ? null : isolated;
            }
            else if (stored != null)
            {
                return isDeleted(stored) ? null : stored;
            }
            return null;
        }

        public Bag get(Unmarshaller unmarshaller, Long key)
        {
            Record record = get(new Comparable[] { key });
            return record == null ? null : unmarshall(unmarshaller, record);
        }

        Bag get(Unmarshaller unmarshaller, Long key, Long version)
        {
            Record record = get(new Comparable[] { key, version });
            return record == null ? null : unmarshall(unmarshaller, record);
        }

        public Join getJoin(String name)
        {
            Join join = (Join) mapOfJoins.get(name);
            if (join == null)
            {
                Join.Schema joinSchema = (Join.Schema) common.schema.mapOfJoinSchemas.get(name);
                join = new Join(snapshot, joinSchema);
                mapOfJoins.put(name, join);
            }
            return join;
        }

        List getRecords()
        {
            List listOfRecords = new ArrayList();
            Strata.Cursor isolated = isolation.first();
            if (isolated.hasNext())
            {
                Record first = (Record) isolated.next();
                while (first != null)
                {
                    Record next = null;
                    Record record = first;
                    for (;;)
                    {
                        if (!isolated.hasNext())
                        {
                            break;
                        }
                        next = (Record) isolated.next();
                        if (!next.key.equals(first.key))
                        {
                            break;
                        }
                        record = next;
                    }
                    listOfRecords.add(record);
                    first = next;
                }
            }
            isolated.release();
            return listOfRecords;
        }

        void commit(Unmarshaller unmarshaller)
        {
            Iterator records = getRecords().iterator();
            while (records.hasNext())
            {
                Record record = (Record) records.next();
                query.insert(record);
            }
            query.flush();

            Iterator joins = mapOfJoins.values().iterator();
            while (joins.hasNext())
            {
                Join join = (Join) joins.next();
                join.commit();
            }

            Iterator indices = mapOfIndices.values().iterator();
            while (indices.hasNext())
            {
                Index index = (Index) indices.next();
                index.commit(snapshot, this, unmarshaller);
            }
        }

        public Iterator find(String string, Unmarshaller unmarshaller, Comparable[] fields)
        {
            Index index = (Index) mapOfIndices.get(string);
            if (index == null)
            {
                // TODO Just a code. You could create something in swag that
                // builds a message from a format, but using the code so that
                // it is not required.
                throw new Exception("no.such.index", 503).add(string);
            }

            return index.find(snapshot, this, unmarshaller, fields);
        }

        private final static class Record
        {
            public final Long key;

            public final Long version;

            public final Bento.Address address;

            public Record(Long key, Long version, Bento.Address address)
            {
                this.key = key;
                this.version = version;
                this.address = address;
            }

            public boolean equals(Object object)
            {
                if (object instanceof Record)
                {
                    Record record = (Record) object;
                    return key.equals(record.key) && version.equals(record.version);
                }
                return false;
            }

            public int hashCode()
            {
                int hashCode = 1;
                hashCode = hashCode * 37 + key.hashCode();
                hashCode = hashCode * 37 + version.hashCode();
                return hashCode;
            }
        }

        private final static class Extractor
        implements Strata.FieldExtractor, Serializable
        {
            private static final long serialVersionUID = 20070408L;

            public Comparable[] getFields(Object txn, Object object)
            {
                Record record = (Record) object;
                // TODO Does the version need to be part of the key? It helps
                // to find the right version from an index, but might not be
                // necessary.
                return new Comparable[] { record.key, record.version };
            }
        }

        private final static class Writer
        implements BentoStorage.Writer, Serializable
        {
            private static final long serialVersionUID = 20070208L;

            public void write(ByteBuffer out, Object object)
            {
                Record record = (Record) object;
                out.putLong(record.key.longValue());
                out.putLong(record.version.longValue());
                out.putLong(record.address.getPosition());
                out.putInt(record.address.getBlockSize());
            }
        }

        private final static class Reader
        implements BentoStorage.Reader, Serializable
        {
            private static final long serialVersionUID = 20070208L;

            public Object read(ByteBuffer in)
            {
                return new Record(new Long(in.getLong()), new Long(in.getLong()), new Bento.Address(in.getLong(), in.getInt()));
            }
        }

        private final static class Schema
        implements Serializable
        {
            private static final long serialVersionUID = 20070408L;

            public final Strata strata;

            public final Map mapOfJoinSchemas;

            public final Map mapOfIndexSchemas;

            public Schema(Strata strata, Map mapOfJoinSchemas, Map mapOfIndexSchemas)
            {
                this.strata = strata;
                this.mapOfJoinSchemas = mapOfJoinSchemas;
                this.mapOfIndexSchemas = mapOfIndexSchemas;
            }
        }

        public final static class Common
        {
            public final Schema schema;

            private long identifier;

            public Common(Schema schema, long identifier)
            {
                this.schema = schema;
                this.identifier = identifier;
            }

            public synchronized Long nextIdentifier()
            {
                return new Long(identifier++);
            }
        }

        public final static class Creator
        {
            private final String name;

            private final Map mapOfIndices;

            private final Map mapOfJoinCreators;

            public Creator(String name)
            {
                this.name = name;
                this.mapOfIndices = new HashMap();
                this.mapOfJoinCreators = new HashMap();
            }

            public String getName()
            {
                return name;
            }

            public Join.Creator newJoin(String name)
            {
                if (mapOfJoinCreators.containsKey(name))
                {
                    throw new IllegalStateException();
                }
                Join.Creator newJoin = new Join.Creator(getName(), getName());
                mapOfJoinCreators.put(name, newJoin);
                return newJoin;
            }

            public void newIndex(String name, FieldExtractor fields)
            {
                mapOfIndices.put(name, fields);
            }
        }

    }

    public final static class Creator
    {
        private final Map mapOfBinCreators = new HashMap();

        public Bin.Creator newBin(String name)
        {
            Bin.Creator newBin = new Bin.Creator(name);
            mapOfBinCreators.put(name, newBin);
            return newBin;
        }

        public Depot create(File file)
        {
            Bento.Creator newBento = new Bento.Creator();
            newBento.addStaticPage(HEADER_URI, Bento.ADDRESS_SIZE);
            Bento bento = newBento.create(file);
            Bento.Mutator mutator = bento.mutate(bento.newNullJournal());

            BentoStorage.Creator newMutationStorage = new BentoStorage.Creator();

            newMutationStorage.setWriter(new Snapshot.Writer());
            newMutationStorage.setReader(new Snapshot.Reader());
            newMutationStorage.setSize(SizeOf.LONG + SizeOf.INTEGER);

            Strata.Creator newMutationStrata = new Strata.Creator();

            newMutationStrata.setStorage(newMutationStorage.create());
            newMutationStrata.setFieldExtractor(new Snapshot.Extractor());

            Object txn = BentoStorage.txn(mutator);
            Strata mutations = newMutationStrata.create(txn);

            Strata.Query query = mutations.query(txn);
            query.insert(new Snapshot.Record(new Long(1L), COMMITTED));

            Map mapOfBins = new HashMap();
            Iterator bags = mapOfBinCreators.entrySet().iterator();
            while (bags.hasNext())
            {
                Map.Entry entry = (Map.Entry) bags.next();
                String name = (String) entry.getKey();

                BentoStorage.Creator newBinStorage = new BentoStorage.Creator();
                newBinStorage.setWriter(new Bin.Writer());
                newBinStorage.setReader(new Bin.Reader());
                newBinStorage.setSize(SizeOf.LONG * 2 + Bento.ADDRESS_SIZE);

                Strata.Creator newBinStrata = new Strata.Creator();

                newBinStrata.setStorage(newBinStorage.create());
                newBinStrata.setFieldExtractor(new Bin.Extractor());

                Strata strata = newBinStrata.create(BentoStorage.txn(mutator));

                Bin.Creator newBin = (Bin.Creator) entry.getValue();

                Map mapOfJoins = new HashMap();
                Iterator joins = newBin.mapOfJoinCreators.entrySet().iterator();
                while (joins.hasNext())
                {
                    Map.Entry join = (Map.Entry) joins.next();
                    String joinName = (String) join.getKey();
                    Join.Creator newJoin = (Join.Creator) join.getValue();
                    Map mapOfFields = new LinkedHashMap(newJoin.mapOfFields);

                    BentoStorage.Creator newJoinStorage = new BentoStorage.Creator();
                    newJoinStorage.setWriter(new Join.Writer(mapOfFields.size()));
                    newJoinStorage.setReader(new Join.Reader(mapOfFields.size()));
                    newJoinStorage.setSize(SizeOf.LONG * mapOfFields.size() + SizeOf.LONG + SizeOf.SHORT);

                    Strata.Creator newJoinStrata = new Strata.Creator();

                    newJoinStrata.setStorage(newJoinStorage.create());
                    newJoinStrata.setFieldExtractor(new Join.Extractor());

                    Strata joinStrata = newJoinStrata.create(BentoStorage.txn(mutator));

                    mapOfJoins.put(joinName, new Join.Schema(joinStrata, mapOfFields));
                }

                Map mapOfIndices = new HashMap();
                Iterator indices = newBin.mapOfIndices.entrySet().iterator();
                while (indices.hasNext())
                {
                    Map.Entry index = (Map.Entry) indices.next();
                    String nameOfIndex = (String) index.getKey();

                    BentoStorage.Creator newIndexStorage = new BentoStorage.Creator();
                    newIndexStorage.setWriter(new Index.Writer());
                    newIndexStorage.setReader(new Index.Reader());
                    newIndexStorage.setSize(SizeOf.LONG + SizeOf.LONG + SizeOf.SHORT);

                    Strata.Creator newJoinStrata = new Strata.Creator();
                    FieldExtractor fields = (FieldExtractor) index.getValue();

                    newJoinStrata.setStorage(newIndexStorage.create());
                    newJoinStrata.setFieldExtractor(new Index.Extractor());

                    Strata indexStrata = newJoinStrata.create(BentoStorage.txn(mutator));

                    mapOfIndices.put(nameOfIndex, new Index.Schema(indexStrata, fields));
                }

                mapOfBins.put(name, new Bin.Schema(strata, mapOfJoins, mapOfIndices));
            }

            Bento.OutputStream allocation = new Bento.OutputStream(mutator);
            try
            {
                ObjectOutputStream out = new ObjectOutputStream(allocation);
                out.writeObject(mutations);
                out.writeObject(mapOfBins);
            }
            catch (IOException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);

            }
            Bento.Address addressOfBins = allocation.allocate(false);

            Bento.Block block = mutator.load(bento.getStaticAddress(HEADER_URI));

            ByteBuffer data = block.toByteBuffer();

            data.putLong(addressOfBins.getPosition());
            data.putInt(addressOfBins.getBlockSize());

            block.write();

            mutator.getJournal().commit();
            bento.close();

            return new Depot.Opener().open(file);
        }
    }

    public final static class Opener
    {
        public Depot open(File file)
        {
            // TODO Implement deletion of temporary blocks.
            Bento.Opener opener = new Bento.Opener(file);
            Bento bento = opener.open();
            Bento.Mutator mutator = bento.mutate();
            Bento.Block block = mutator.load(bento.getStaticAddress(HEADER_URI));
            ByteBuffer data = block.toByteBuffer();
            Bento.Address addressOfBags = new Bento.Address(data.getLong(), data.getInt());
            Strata mutations = null;
            Map mapOfBinSchemas = null;
            try
            {
                ObjectInputStream objects = new ObjectInputStream(new ByteBufferInputStream(mutator.load(addressOfBags).toByteBuffer(), false));
                mutations = (Strata) objects.readObject();
                mapOfBinSchemas = (Map) objects.readObject();
            }
            catch (IOException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }
            catch (ClassNotFoundException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }

            Map mapOfBinCommons = new HashMap();
            Iterator bins = mapOfBinSchemas.entrySet().iterator();
            while (bins.hasNext())
            {
                Map.Entry entry = (Map.Entry) bins.next();
                Bin.Schema binSchema = (Bin.Schema) entry.getValue();

                long identifer = 1L;
                Strata.Query query = binSchema.strata.query(BentoStorage.txn(mutator));
                Strata.Cursor last = query.last();
                // TODO You can use hasPrevious when it is implemented.
                while (last.hasNext())
                {
                    Bin.Record record = (Bin.Record) last.next();
                    identifer = record.key.longValue() + 1;
                }
                last.release();

                mapOfBinCommons.put(entry.getKey(), new Bin.Common(binSchema, identifer));
            }

            return new Depot(file, bento, mutations, mapOfBinCommons);
        }
    }

    public interface Marshaller
    {
        public void marshall(OutputStream out, Object object);
    }

    public final static class SerializationMarshaller
    implements Marshaller
    {
        public void marshall(OutputStream out, Object object)
        {
            try
            {
                new ObjectOutputStream(out).writeObject(object);
            }
            catch (IOException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }
        }
    }

    public interface Unmarshaller
    {
        public Object unmarshall(InputStream in);
    }

    public final static class SerializationUnmarshaller
    implements Unmarshaller
    {
        public Object unmarshall(InputStream in)
        {
            Object object;
            try
            {
                object = new ObjectInputStream(in).readObject();
            }
            catch (IOException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }
            catch (ClassNotFoundException e)
            {
                throw new Exception("class.not.found", 403);
            }
            return object;
        }
    }

    public final static class Join
    {
        private final Snapshot snapshot;

        private final Schema schema;

        private final Strata.Query query;

        private final Strata.Query isolation;

        public Join(Snapshot snapshot, Schema schema)
        {
            this.snapshot = snapshot;
            this.query = schema.strata.query(snapshot);
            this.isolation = newIsolation();
            this.schema = schema;
        }

        private static Strata.Query newIsolation()
        {
            Strata.Creator creator = new Strata.Creator();

            creator.setCacheFields(true);
            creator.setFieldExtractor(new Extractor());
            creator.setStorage(new ArrayListStorage());

            return creator.create(null).query(null);
        }

        public void unlink(Bag[] bags)
        {
            Long[] keys = new Long[bags.length];

            for (int i = 0; i < bags.length; i++)
            {
                keys[i] = bags[i].getKey();
            }

            unlink(keys);
        }

        public void unlink(Long[] keys)
        {
            add(keys, snapshot.getVersion(), true);
        }

        public void link(Long[] keys)
        {
            add(keys, snapshot.getVersion(), false);
        }

        public void link(Bag[] bags)
        {
            Long[] keys = new Long[bags.length];

            for (int i = 0; i < bags.length; i++)
            {
                keys[i] = bags[i].getKey();
            }

            add(keys, snapshot.getVersion(), false);
        }

        public void add(Long[] keys, Long version, boolean deleted)
        {
            isolation.insert(new Record(keys, version, deleted));
        }

        public java.util.Iterator find(Bag[] bags)
        {
            Long[] keys = new Long[bags.length];

            for (int i = 0; i < bags.length; i++)
            {
                keys[i] = bags[i].getKey();
            }

            return find(keys);
        }

        public java.util.Iterator find(Long[] keys)
        {
            return new Iterator(snapshot, keys, query.find(keys), isolation.find(keys), schema);
        }

        private void commit()
        {
            Strata.Cursor isolated = isolation.first();
            if (isolated.hasNext())
            {
                Record first = (Record) isolated.next();
                while (first != null)
                {
                    Record next = null;
                    Record record = first;
                    for (;;)
                    {
                        if (!isolated.hasNext())
                        {
                            break;
                        }
                        next = (Record) isolated.next();
                        if (!partial(next.keys, first.keys))
                        {
                            break;
                        }
                        record = next;
                    }
                    query.insert(record);
                    first = next;
                }
            }
            isolated.release();
            query.flush();
        }

        private final static class Record
        {
            public final Long[] keys;

            public final Long version;

            public final boolean deleted;

            public Record(Long[] keys, Long version, boolean deleted)
            {
                this.keys = keys;
                this.version = version;
                this.deleted = deleted;
            }

            public boolean equals(Object object)
            {
                if (object instanceof Record)
                {
                    Record record = (Record) object;
                    Long[] left = keys;
                    Long[] right = record.keys;
                    if (left.length != right.length)
                    {
                        return false;
                    }
                    for (int i = 0; i < left.length; i++)
                    {
                        if (!left[i].equals(right[i]))
                        {
                            return false;
                        }
                    }
                    return version.equals(record.version) && deleted == record.deleted;
                }
                return false;
            }

            public int hashCode()
            {
                int hashCode = 1;
                for (int i = 0; i < keys.length; i++)
                {
                    hashCode = hashCode * 37 + keys[i].hashCode();
                }
                hashCode = hashCode * 37 + version.hashCode();
                hashCode = hashCode * 37 + (deleted ? 1 : 0);
                return hashCode;
            }
        }

        private final static class Extractor
        implements Strata.FieldExtractor, Serializable
        {
            private static final long serialVersionUID = 20070403L;

            public Comparable[] getFields(Object txn, Object object)
            {
                Record record = (Record) object;
                Comparable[] fields = new Comparable[record.keys.length + 2];
                Long[] keys = record.keys;
                for (int i = 0; i < keys.length; i++)
                {
                    fields[i] = keys[i];
                }
                fields[keys.length] = record.version;
                fields[keys.length + 1] = record.deleted ? new Integer(1) : new Integer(0);
                return fields;
            }
        }

        private final static class Writer
        implements BentoStorage.Writer, Serializable
        {
            private static final long serialVersionUID = 20070208L;

            private final int size;

            public Writer(int size)
            {
                this.size = size;
            }

            public void write(ByteBuffer bytes, Object object)
            {
                Record record = (Record) object;
                Long[] keys = record.keys;
                for (int i = 0; i < size; i++)
                {
                    bytes.putLong(keys[i].longValue());
                }
                bytes.putLong(record.version.longValue());
                bytes.putShort(record.deleted ? (short) 1 : (short) 0);
            }
        }

        private final static class Reader
        implements BentoStorage.Reader, Serializable
        {
            private static final long serialVersionUID = 20070208L;

            private final int size;

            public Reader(int size)
            {
                this.size = size;
            }

            public Object read(ByteBuffer bytes)
            {
                Long[] keys = new Long[size];
                for (int i = 0; i < size; i++)
                {
                    keys[i] = new Long(bytes.getLong());
                }
                return new Record(keys, new Long(bytes.getLong()), bytes.getShort() == 1);
            }
        }

        private final static class Schema
        implements Serializable
        {
            private static final long serialVersionUID = 20070208L;

            public final Map mapOfFields;

            public final Strata strata;

            public Schema(Strata strata, Map mapOfFields)
            {
                this.mapOfFields = mapOfFields;
                this.strata = strata;
            }
        }

        public final static class Creator
        {
            private final HashMap mapOfFields = new LinkedHashMap();

            public Creator(String fieldName, String binName)
            {
                mapOfFields.put(fieldName, binName);
            }

            public Creator add(String fieldName, Bin.Creator newBin)
            {
                mapOfFields.put(fieldName, newBin.getName());
                return this;
            }

            public Creator add(Bin.Creator newBin)
            {
                mapOfFields.put(newBin.getName(), newBin.getName());
                return this;
            }
        }

        private final static class Iterator
        implements java.util.Iterator
        {
            private final Strata.Cursor stored;

            private final Strata.Cursor isolated;

            private final Snapshot snapshot;

            private final Long[] keys;

            private final Map.Entry[] fieldMappings;

            private Join.Record nextStored;

            private Join.Record nextIsolated;

            private Join.Record next;

            public Iterator(Snapshot snapshot, Long[] keys, Strata.Cursor stored, Strata.Cursor isolated, Schema schema)
            {
                this.snapshot = snapshot;
                this.keys = keys;
                this.stored = stored;
                this.isolated = isolated;
                this.nextStored = next(stored, false);
                this.nextIsolated = next(isolated, true);
                this.next = nextRecord();
                this.fieldMappings = (Map.Entry[]) schema.mapOfFields.entrySet().toArray(new Map.Entry[schema.mapOfFields.size()]);
            }

            private Record next(Strata.Cursor cursor, boolean isolated)
            {
                Record candidate = null;
                Long[] candidateKeys = null;
                for (;;)
                {
                    if (!cursor.hasNext())
                    {
                        break;
                    }
                    Record record = (Record) cursor.next();
                    if (!partial(keys, record.keys))
                    {
                        break;
                    }
                    if (isolated || snapshot.isVisible(record.version))
                    {
                        if (candidateKeys == null)
                        {
                            candidateKeys = record.keys;
                        }
                        else if (!partial(candidateKeys, record.keys))
                        {
                            continue;
                        }
                        candidate = record;
                    }
                }
                return candidate;
            }

            private Record nextRecord()
            {
                Record next = null;
                if (nextIsolated != null || nextStored != null)
                {
                    if (nextIsolated == null)
                    {
                        next = nextStored;
                        nextStored = next(stored, false);
                    }
                    else if (nextStored == null)
                    {
                        next = nextIsolated;
                        nextIsolated = next(isolated, true);
                    }
                    else
                    {
                        int compare = compare(nextIsolated.keys, nextStored.keys);
                        if (compare < 0)
                        {
                            next = nextIsolated;
                            nextIsolated = next(isolated, true);
                        }
                        else if (compare > 0)
                        {
                            next = nextStored;
                            nextStored = next(stored, false);
                        }
                        else
                        {
                            next = nextIsolated;
                            nextIsolated = next(isolated, true);
                            nextStored = next(stored, true);
                        }
                    }
                    if (next.deleted)
                    {
                        next = nextRecord();
                    }
                }
                return next;
            }

            public boolean hasNext()
            {
                return next != null;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public Object next()
            {
                Tuple tuple = new Tuple(snapshot, fieldMappings, next);
                next = nextRecord();
                return tuple;
            }
        }

    }

    public final static class Tuple
    {
        private final Snapshot snapshot;

        private final Map.Entry[] fieldMappings;

        private final Join.Record record;

        public Tuple(Snapshot snapshot, Map.Entry[] fieldMappings, Join.Record record)
        {
            this.snapshot = snapshot;
            this.fieldMappings = fieldMappings;
            this.record = record;
        }

        public Bag getBag(Unmarshaller unmarshaller, int i)
        {
            String bagName = (String) fieldMappings[i].getValue();
            return snapshot.getBin(bagName).get(unmarshaller, record.keys[i]);
        }
    }

    public interface FieldExtractor
    extends Serializable
    {
        public abstract Comparable[] getFields(Object object);
    }

    private final static class Index
    {
        private final Schema schema;

        private final Strata isolation;

        public Index(Schema schema)
        {
            this.schema = schema;
            this.isolation = newIsolation();
        }

        private static Strata newIsolation()
        {
            Strata.Creator creator = new Strata.Creator();

            creator.setCacheFields(true);
            creator.setFieldExtractor(new Extractor());
            creator.setStorage(new ArrayListStorage());

            return creator.create(null);
        }

        public void add(Bin bin, Bento.Mutator mutator, Unmarshaller unmarshaller, Bag bag)
        {
            Transaction txn = new Transaction(mutator, bin, unmarshaller, schema.extractor);
            Strata.Query query = isolation.query(txn);
            Record record = new Record(bag.getKey(), bag.getVersion());
            query.insert(record);
            query.flush();
        }

        public void update(Bin bin, Bento.Mutator mutator, Unmarshaller unmarshaller, Long key, Long previous, Long next)
        {
            Transaction txn = new Transaction(mutator, bin, unmarshaller, schema.extractor);
            Strata.Query query = isolation.query(txn);
            Strata.Cursor found = query.find(txn.getFields(key, previous));
            while (found.hasNext())
            {
                Record record = (Record) found.next();
                if (record.key.equals(key) && record.version.equals(previous))
                {
                    found.release();
                    query.remove(record);
                    query.flush();
                    break;
                }
            }
            found.release();
            query.insert(new Record(key, next));
            query.flush();
        }

        public final static class Record
        {
            public final Long key;

            public final Long version;

            public Record(Long key, Long version)
            {
                this.key = key;
                this.version = version;
            }

            public boolean equals(Object object)
            {
                if (object instanceof Record)
                {
                    Record record = (Record) object;
                    return key.equals(record.key) && version.equals(record.version);
                }
                return false;
            }

            public int hashCode()
            {
                int hashCode = 1;
                hashCode = hashCode * 37 + key.hashCode();
                hashCode = hashCode * 37 + version.hashCode();
                return hashCode;
            }
        }

        public final static class Extractor
        implements Strata.FieldExtractor, Serializable
        {
            private static final long serialVersionUID = 20070403L;

            public Comparable[] getFields(Object txn, Object object)
            {
                Record record = (Record) object;
                Transaction transaction = (Transaction) txn;
                return transaction.getFields(record.key, record.version);
            }
        }

        public final static class Writer
        implements BentoStorage.Writer, Serializable
        {
            private static final long serialVersionUID = 20070208L;

            public void write(ByteBuffer bytes, Object object)
            {
                Record record = (Record) object;
                bytes.putLong(record.key.longValue());
                bytes.putLong(record.version.longValue());
            }
        }

        public final static class Reader
        implements BentoStorage.Reader, Serializable
        {
            private static final long serialVersionUID = 20070208L;

            public Object read(ByteBuffer bytes)
            {
                return new Record(new Long(bytes.getLong()), new Long(bytes.getLong()));
            }
        }

        public final static class Schema
        implements Serializable
        {
            private static final long serialVersionUID = 20070610L;

            public final FieldExtractor extractor;

            public final Strata strata;

            public Schema(Strata strata, FieldExtractor fields)
            {
                this.extractor = fields;
                this.strata = strata;
            }
        }

        public final static class Creator
        {
            private final Strata.FieldExtractor fields;

            public Creator(Strata.FieldExtractor fields)
            {
                this.fields = fields;
            }

            public Strata.FieldExtractor getFields()
            {
                return fields;
            }
        }

        public final static class Transaction
        implements BentoStorage.MutatorServer
        {
            public final Bento.Mutator mutator;

            public final Bin bin;

            public final Unmarshaller unmarshaller;

            public final FieldExtractor extractor;

            public Transaction(Bento.Mutator mutator, Bin bin, Unmarshaller unmarshaller, FieldExtractor extractor)
            {
                this.mutator = mutator;
                this.bin = bin;
                this.unmarshaller = unmarshaller;
                this.extractor = extractor;
            }

            public Bento.Mutator getMutator()
            {
                return mutator;
            }

            public Comparable[] getFields(Long key, Long version)
            {
                return extractor.getFields(bin.get(unmarshaller, key, version).getObject());
            }

            public Bag getBag(Record record)
            {
                return bin.get(unmarshaller, record.key);
            }

            public boolean isDeleted(Record record)
            {
                return !record.version.equals(bin.get(unmarshaller, record.key).getVersion());
            }
        }

        public Iterator find(Snapshot snapshot, Bin bin, Unmarshaller unmarshaller, Comparable[] fields)
        {
            Transaction txn = new Transaction(snapshot.getMutator(), bin, unmarshaller, schema.extractor);
            return new Cursor(schema.strata.query(txn).find(fields), isolation.query(txn).find(fields), txn, fields);
        }

        private void commit(Snapshot snapshot, Bin bin, Unmarshaller unmarshaller)
        {
            Transaction txn = new Transaction(snapshot.getMutator(), bin, unmarshaller, schema.extractor);
            Strata.Query queryOfIsolated = isolation.query(txn);
            Strata.Query queryOfStored = schema.strata.query(txn);
            Strata.Cursor isolated = queryOfIsolated.first();
            while (isolated.hasNext())
            {
                Record record = (Record) isolated.next();
                queryOfStored.insert(record);
            }
            isolated.release();
            queryOfStored.flush();
        }

        public final static class Cursor
        implements Iterator
        {
            private final Comparable[] fields;

            private final Transaction txn;

            private final Strata.Cursor isolated;

            private final Strata.Cursor stored;

            private Record nextStored;

            private Record nextIsolated;

            private Bag next;

            public Cursor(Strata.Cursor stored, Strata.Cursor isolated, Transaction txn, Comparable[] fields)
            {
                this.txn = txn;
                this.fields = fields;
                this.isolated = isolated;
                this.stored = stored;
                this.nextStored = next(stored, false);
                this.nextIsolated = next(isolated, true);
                this.next = nextBag();
            }

            private Record next(Strata.Cursor cursor, boolean isolated)
            {
                while (cursor.hasNext())
                {
                    Record record = (Record) cursor.next();
                    Bag bag = txn.bin.get(txn.unmarshaller, record.key);
                    if (bag == null || !bag.getVersion().equals(record.version))
                    {
                        continue;
                    }
                    if (!partial(fields, txn.extractor.getFields(bag.getObject())))
                    {
                        return null;
                    }
                    return record;
                }
                cursor.release();
                return null;
            }

            private Bag nextBag()
            {
                Bag bag = null;
                if (nextIsolated != null || nextStored != null)
                {
                    Record next = null;
                    if (nextIsolated == null)
                    {
                        next = nextStored;
                        nextStored = next(stored, false);
                    }
                    else if (nextStored == null)
                    {
                        next = nextIsolated;
                        nextIsolated = next(isolated, true);
                    }
                    else
                    {
                        int compare = compare(txn.getFields(nextIsolated.key, nextIsolated.version), txn.getFields(nextStored.key, nextStored.version));
                        if (compare < 0)
                        {
                            next = nextIsolated;
                            nextIsolated = next(isolated, true);
                        }
                        else if (compare > 0)
                        {
                            next = nextStored;
                            nextStored = next(stored, false);
                        }
                        else
                        {
                            next = nextIsolated;
                            nextIsolated = next(isolated, true);
                            nextStored = next(stored, true);
                        }
                    }
                    bag = txn.getBag(next);
                    if (!bag.getVersion().equals(next.version))
                    {
                        bag = nextBag();
                    }
                }
                return bag;
            }

            public boolean hasNext()
            {
                return next != null;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public Object next()
            {
                Bag bag = next;
                next = nextBag();
                return bag;
            }

            public void release()
            {
                isolated.release();
                stored.release();
            }
        }
    }

    public final static class Snapshot
    implements BentoStorage.MutatorServer
    {
        private final Strata snapshots;

        private final Map mapOfBinCommons;

        private final Set setOfCommitted;

        private final Bento.Mutator mutator;

        private final Map mapOfBins;

        private final Long version;

        private final Test test;

        private final Long oldest;

        private boolean spent;

        private int binNameSize;

        public Snapshot(Strata snapshots, Map mapOfBinCommons, Bento.Mutator mutator, Set setOfCommitted, Test test, Long version)
        {
            this.snapshots = snapshots;
            this.mapOfBinCommons = mapOfBinCommons;
            this.mutator = mutator;
            this.mapOfBins = new HashMap();
            this.version = version;
            this.test = test;
            this.setOfCommitted = setOfCommitted;
            this.oldest = (Long) setOfCommitted.iterator().next();
        }

        public Bin getBin(String name)
        {
            Bin bin = (Bin) mapOfBins.get(name);
            if (bin == null)
            {
                Bin.Common binCommon = (Bin.Common) mapOfBinCommons.get(name);
                bin = new Bin(this, name, binCommon);
                mapOfBins.put(name, bin);
                binNameSize += SizeOf.SHORT + (SizeOf.CHAR * name.length());
            }
            return bin;
        }

        public Long getVersion()
        {
            return version;
        }

        public boolean isVisible(Long version)
        {
            if (oldest.compareTo(version) >= 0)
            {
                return true;
            }
            if (setOfCommitted.contains(version))
            {
                return true;
            }
            return false;
        }

        private long checksum(ByteBuffer out)
        {
            Checksum checksum = new Adler32();

            while (out.remaining() != 0)
            {
                checksum.update(out.get());
            }

            return checksum.getValue();
        }

        public void commit(Unmarshaller unmarshaller)
        {
            if (spent)
            {
                throw new Exception("commit.spent.snapshot", 501);
            }

            spent = true;

            Bin[] bins = (Bin[]) mapOfBins.values().toArray(new Bin[mapOfBins.size()]);

            int size = SizeOf.LONG + SizeOf.LONG + SizeOf.INTEGER + SizeOf.INTEGER;
            size += binNameSize;

            int allocations = 0;

            for (int i = 0; i < bins.length; i++)
            {
                size += (SizeOf.LONG + Bento.ADDRESS_SIZE + SizeOf.SHORT) * bins[i].allocations;
                allocations += bins[i].allocations;
            }

            Bento.Block temporary = mutator.temporary(size);
            ByteBuffer out = temporary.toByteBuffer();

            out.position(SizeOf.LONG);

            out.putLong(version.longValue());
            out.putInt(mapOfBins.size());
            out.putInt(allocations);

            for (int i = 0; i < bins.length; i++)
            {
                String name = bins[i].getName();
                out.putShort((short) name.length());
                for (int j = 0; j < name.length(); j++)
                {
                    out.putChar(name.charAt(j));
                }
            }

            for (short i = 0; i < bins.length; i++)
            {
                Iterator records = bins[i].getRecords().iterator();
                while (records.hasNext())
                {
                    Bin.Record record = (Bin.Record) records.next();
                    if (!Bento.NULL_ADDRESS.equals(record.address))
                    {
                        out.putLong(record.key.longValue());
                        out.putLong(record.address.getPosition());
                        out.putLong(record.address.getBlockSize());
                        out.putShort(i);
                    }
                }
            }

            out.flip();
            out.position(SizeOf.LONG);

            long checksum = checksum(out);

            out.position(0);
            out.putLong(checksum);

            temporary.write();

            mutator.getJournal().commit();

            for (int i = 0; i < bins.length; i++)
            {
                bins[i].commit(unmarshaller);
            }

            test.changesWritten.release();
            try
            {
                test.registerMutation.acquire();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }

            mutator.free(mutator.load(temporary.getAddress()));

            Strata.Query query = snapshots.query(BentoStorage.txn(mutator));

            Record committed = new Record(version, COMMITTED);
            query.insert(committed);

            query.remove(new Comparable[] { version }, Strata.ANY);

            test.journalComplete.release();
        }

        public void rollback()
        {
            if (spent)
            {
                throw new Exception("rollback.spent.snapshot", 502);
            }

            spent = true;

            mutator.getJournal().rollback();

            Strata.Query query = snapshots.query(BentoStorage.txn(mutator));
            query.remove(new Comparable[] { version }, Strata.ANY);
        }

        public Bento.Mutator getMutator()
        {
            return mutator;
        }

        private final static class Record
        {
            public final Long version;

            public final Integer state;

            public Record(Long version, Integer state)
            {
                this.version = version;
                this.state = state;
            }

            public boolean equals(Object object)
            {
                if (object instanceof Record)
                {
                    Record record = (Record) object;
                    return version.equals(record.version) && state.equals(record.state);
                }
                return false;
            }

            public int hashCode()
            {
                int hashCode = 1;
                hashCode = hashCode * 37 + version.hashCode();
                hashCode = hashCode * 37 + state.hashCode();
                return hashCode;
            }
        }

        private final static class Extractor
        implements Strata.FieldExtractor, Serializable
        {
            private static final long serialVersionUID = 20070409L;

            public Comparable[] getFields(Object txn, Object object)
            {
                Record record = (Record) object;
                return new Comparable[] { record.version };
            }
        }

        private final static class Writer
        implements BentoStorage.Writer, Serializable
        {
            private static final long serialVersionUID = 20070409L;

            public void write(ByteBuffer bytes, Object object)
            {
                if (object == null)
                {
                    bytes.putLong(0L);
                    bytes.putInt(0);
                }
                else
                {
                    Record record = (Record) object;
                    bytes.putLong(record.version.longValue());
                    bytes.putInt(record.state.intValue());
                }
            }
        }

        private final static class Reader
        implements BentoStorage.Reader, Serializable
        {
            private static final long serialVersionUID = 20070409L;

            public Object read(ByteBuffer bytes)
            {
                Long version = new Long(bytes.getLong());
                Integer state = new Integer(bytes.getInt());
                if (version.longValue() == 0L)
                {
                    return null;
                }
                Record record = new Record(version, state);
                return record;
            }
        }
    }

    public final static class Test
    {
        private Sync registerMutation;

        private Sync changesWritten;

        private Sync journalComplete;

        public Test()
        {
            this.changesWritten = new NullSync();
            this.registerMutation = new NullSync();
            this.journalComplete = new NullSync();
        }

        public void setJournalLatches(Sync changesWritten, Sync registerMutation)
        {
            this.changesWritten = changesWritten;
            this.registerMutation = registerMutation;
        }

        public void setJournalComplete(Sync journalComplete)
        {
            this.journalComplete = journalComplete;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */