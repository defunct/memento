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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import EDU.oswego.cs.dl.util.concurrent.NullSync;
import EDU.oswego.cs.dl.util.concurrent.Sync;

import com.agtrz.bento.Bento;
import com.agtrz.bento.Bento.Mutator;
import com.agtrz.strata.ArrayListStorage;
import com.agtrz.strata.Strata;
import com.agtrz.strata.bento.BentoStorage;
import com.agtrz.swag.danger.AsinineCheckedExceptionThatIsEntirelyImpossible;
import com.agtrz.swag.danger.Danger;
import com.agtrz.swag.io.ByteBufferInputStream;
import com.agtrz.swag.io.SizeOf;

public class Depot
{
    private final static URI HEADER_URI = URI.create("http://syndibase.agtrz.com/strata");

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

    private final static Integer OPERATING = new Integer(0);

    private final static Integer COMMITTED = new Integer(1);

    public final static class Mutation
    {
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

    public final static class Bin
    {
        private final String name;

        private final Snapshot snapshot;

        private final Common common;

        private final Strata.Query query;

        private final Map mapOfJoins = new HashMap();

        private final Strata.Query isolation;

        public final Map mapOfObjects = new LinkedHashMap();

        public Bin(Snapshot snapshot, String name, Common common)
        {
            this.snapshot = snapshot;
            this.name = name;
            this.common = common;
            this.query = common.strata.query(snapshot);
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

        public String getName()
        {
            return name;
        }

        public Bag add(Marshaller marshaller, Object object)
        {
            Bag bag = new Bag(this, common.nextIdentifier(), snapshot.getVersion(), object);
            Bento.OutputStream allocation = new Bento.OutputStream(snapshot.mutator);
            marshaller.marshall(allocation, object);
            Bento.Address address = allocation.allocate(false);
            isolation.insert(new Record(bag.getKey(), bag.getVersion(), address));
            return bag;
        }

        public Bag update(Marshaller marshaller, Long key, Object object)
        {
            Record record = get(key);
            if (record == null)
            {
                Danger danger = new Danger();

                danger.source(Depot.class);
                danger.message("update.bag.does.not.exist");

                throw danger;
            }
            Bag bag = new Bag(this, key, snapshot.getVersion(), object);
            Bento.OutputStream allocation = new Bento.OutputStream(snapshot.mutator);
            marshaller.marshall(allocation, object);
            Bento.Address address = allocation.allocate(false);
            isolation.insert(new Record(bag.getKey(), bag.getVersion(), address));
            return bag;
        }

        public void delete(Long key)
        {
            Record record = get(key);
            if (record == null)
            {
                Danger danger = new Danger();

                danger.source(Depot.class);
                danger.message("delete.bag.does.not.exist");

                throw danger;
            }
            isolation.insert(new Record(key, snapshot.getVersion(), Bento.NULL_ADDRESS));
        }

        private Record get(Strata.Cursor cursor, Long key, boolean isolated)
        {
            Record candidate = null;
            for (;;)
            {
                if (!cursor.hasNext())
                {
                    break;
                }
                Record record = (Record) cursor.next();
                if (!record.key.equals(key))
                {
                    break;
                }
                if (isolated || snapshot.isVisible(record.version))
                {
                    candidate = record;
                }
            }
            return candidate;
        }

        private static boolean isDeleted(Record record)
        {
            return record.address.equals(Bento.NULL_ADDRESS);
        }

        private Bag unmarshall(Unmarshaller unmarshaller, Record record)
        {
            Bento.Block block = snapshot.getMutator().load(record.address);
            Object object = unmarshaller.unmarshall(new ByteBufferInputStream(block.toByteBuffer(), false));
            return new Bag(this, record.key, record.version, object);
        }

        private Record get(Long key)
        {
            Record stored = get(query.find(new Comparable[] { key }), key, false);
            Record isolated = get(isolation.find(new Comparable[] { key }), key, true);
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
            Record record = get(key);
            return record == null ? null : unmarshall(unmarshaller, record);
        }

        public Join getJoin(String name)
        {
            Join join = (Join) mapOfJoins.get(name);
            if (join == null)
            {
                Join.Common joinCommon = (Join.Common) common.mapOfJoinCommons.get(name);
                join = new Join(snapshot, joinCommon);
                mapOfJoins.put(name, join);
            }
            return join;
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
                        if (!next.key.equals(first.key))
                        {
                            break;
                        }
                        record = next;
                    }
                    query.insert(record);
                    first = next;
                }
            }

            Iterator joins = mapOfJoins.values().iterator();
            while (joins.hasNext())
            {
                Join join = (Join) joins.next();
                join.commit();
            }
            mapOfJoins.clear();
        }

        private void rollback()
        {
        }

        public Index getIndex(String string)
        {
            return null;
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

            public final Map mapOfJoins;

            public final Map mapOfIndices;

            public Schema(Strata strata, Map mapOfJoins, Map mapOfIndices)
            {
                this.strata = strata;
                this.mapOfJoins = mapOfJoins;
                this.mapOfIndices = mapOfIndices;
            }
        }

        public final static class Common
        {
            public final Strata strata;

            public final Map mapOfJoinCommons;

            private long identifier;

            public Common(Strata strata, Map mapOfJoinCommons, long identifier)
            {
                this.strata = strata;
                this.mapOfJoinCommons = mapOfJoinCommons;
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

            newMutationStorage.setWriter(new Mutation.Writer());
            newMutationStorage.setReader(new Mutation.Reader());
            newMutationStorage.setSize(SizeOf.LONG + SizeOf.INTEGER);

            Strata.Creator newMutationStrata = new Strata.Creator();

            newMutationStrata.setStorage(newMutationStorage.create());
            newMutationStrata.setFieldExtractor(new Mutation.Extractor());

            Object txn = BentoStorage.txn(mutator);
            Strata mutations = newMutationStrata.create(txn);

            Strata.Query query = mutations.query(txn);
            query.insert(new Mutation.Record(new Long(1L), COMMITTED));

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
                    newJoinStrata.setFieldExtractor(new Index.Extractor(fields));

                    Strata indexStrata = newJoinStrata.create(BentoStorage.txn(mutator));

                    mapOfIndices.put(nameOfIndex, indexStrata);

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
            Bento bento = new Bento.Opener(file).open();
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
                Map.Entry bin = (Map.Entry) bins.next();
                Bin.Schema binSchema = (Bin.Schema) bin.getValue();
                Map mapOfJoinCommons = new HashMap();
                Iterator joins = binSchema.mapOfJoins.entrySet().iterator();
                while (joins.hasNext())
                {
                    Map.Entry join = (Map.Entry) joins.next();
                    Join.Schema joinSchema = (Join.Schema) join.getValue();
                    Join.Common joinCommon = new Join.Common(joinSchema.strata, new LinkedHashMap(joinSchema.mapOfFields));
                    mapOfJoinCommons.put(join.getKey(), joinCommon);
                }

                long identifer = 1L;
                Strata.Query query = binSchema.strata.query(BentoStorage.txn(mutator));
                Strata.Cursor last = query.last();
                // TODO You can use hasPrevious when it is implemented.
                while (last.hasNext())
                {
                    Bin.Record record = (Bin.Record) last.next();
                    identifer = record.key.longValue() + 1;
                }
                Bin.Common binCommon = new Bin.Common(binSchema.strata, mapOfJoinCommons, identifer);
                mapOfBinCommons.put(bin.getKey(), binCommon);
            }

            return new Depot(file, bento, mutations, mapOfBinCommons);
        }
    }

    public interface Marshaller
    {
        public void marshall(OutputStream out, Object object);
    }

    public final static class SerialzationMarshaller
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
                Danger danger = new DepotException(e);

                danger.source(Depot.class);
                danger.message("class.not.found");

                throw danger;
            }
            return object;
        }
    }

    public final static class Join
    {
        private final Snapshot snapshot;

        private final Common common;

        private final Strata.Query query;

        private final Strata.Query isolation;

        public Join(Snapshot snapshot, Common common)
        {
            this.snapshot = snapshot;
            this.query = common.strata.query(snapshot);
            this.isolation = newIsolation();
            this.common = common;
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
            return new Iterator(snapshot, keys, query.find(keys), isolation.find(keys), common);
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

        private final static class Common
        {
            public final Map mapOfFields;

            public final Strata strata;

            public Common(Strata strata, Map mapOfFields)
            {
                this.strata = strata;
                this.mapOfFields = mapOfFields;
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

            public Iterator(Snapshot snapshot, Long[] keys, Strata.Cursor stored, Strata.Cursor isolated, Common common)
            {
                this.snapshot = snapshot;
                this.keys = keys;
                this.stored = stored;
                this.isolated = isolated;
                this.nextStored = next(stored, false);
                this.nextIsolated = next(isolated, true);
                this.next = nextRecord();
                this.fieldMappings = (Map.Entry[]) common.mapOfFields.entrySet().toArray(new Map.Entry[common.mapOfFields.size()]);
            }

            private Join.Record next(Strata.Cursor cursor, boolean isolated)
            {
                Join.Record candidate = null;
                Long[] candidateKeys = null;
                for (;;)
                {
                    if (!cursor.hasNext())
                    {
                        break;
                    }
                    Join.Record record = (Join.Record) cursor.next();
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

            private Join.Record nextRecord()
            {
                Join.Record next = null;
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

    private static boolean partial(Long[] partial, Long[] full)
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

    private static int compare(Long[] left, Long[] right)
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

    public interface FieldExtractor
    extends Serializable
    {
        public abstract Comparable[] getFields(Object object);
    }

    public final static class Index
    {
        public Index()
        {
        }

        public final static class Record
        {
            public final Long key;

            public final Long version;

            public final boolean deleted;

            public Record(Long key, Long version, boolean deleted)
            {
                this.key = key;
                this.version = version;
                this.deleted = deleted;
            }

            public boolean equals(Object object)
            {
                if (object instanceof Record)
                {
                    Record record = (Record) object;
                    return key.equals(record.key) && version.equals(record.version) && deleted == record.deleted;
                }
                return false;
            }

            public int hashCode()
            {
                int hashCode = 1;
                hashCode = hashCode * 37 + key.hashCode();
                hashCode = hashCode * 37 + version.hashCode();
                hashCode = hashCode * 37 + (deleted ? 1 : 0);
                return hashCode;
            }
        }

        public final static class Extractor
        implements Strata.FieldExtractor, Serializable
        {
            private static final long serialVersionUID = 20070403L;

            private final FieldExtractor fields;

            public Extractor(FieldExtractor fields)
            {
                this.fields = fields;
            }

            public Comparable[] getFields(Object txn, Object object)
            {
                Record record = (Record) object;
                Transaction transaction = (Transaction) txn;
                Bag bag = transaction.bin.get(transaction.unmarshaller, record.key);
                return fields.getFields(bag.getObject());
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
                bytes.putShort(record.deleted ? (short) 1 : (short) 0);
            }
        }

        public final static class Reader
        implements BentoStorage.Reader, Serializable
        {
            private static final long serialVersionUID = 20070208L;

            public Object read(ByteBuffer bytes)
            {
                return new Record(new Long(bytes.getLong()), new Long(bytes.getLong()), bytes.getShort() == 1);
            }
        }

        public final static class Schema
        implements Serializable
        {
            private static final long serialVersionUID = 20070208L;

            public final Strata.FieldExtractor fields;

            public final Strata strata;

            public Schema(Strata strata, Strata.FieldExtractor fields)
            {
                this.fields = fields;
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

            public Transaction(Bento.Mutator mutator, Bin bin, Unmarshaller unmarshaller)
            {
                this.mutator = mutator;
                this.bin = bin;
                this.unmarshaller = unmarshaller;
            }

            public Mutator getMutator()
            {
                return mutator;
            }
        }

        public void find(Comparable[] comparables)
        {
        }
    }

    private final Map mapOfBinCommons;

    private final Bento bento;

    private final Strata mutations;

    public Depot(File file, Bento bento, Strata mutations, Map mapOfBinCommons)
    {
        this.mapOfBinCommons = mapOfBinCommons;
        this.mutations = mutations;
        this.bento = bento;
    }

    public void close()
    {
        bento.close();
    }

    public synchronized Snapshot newSnapshot(Test test)
    {
        Long version = new Long(System.currentTimeMillis());
        Mutation.Record record = new Mutation.Record(version, OPERATING);
        Bento.Mutator mutator = bento.mutate();

        Strata.Query query = mutations.query(BentoStorage.txn(mutator));

        Set setOfCommitted = new TreeSet();
        Strata.Cursor versions = query.first();
        while (versions.hasNext())
        {
            Mutation.Record mutation = (Mutation.Record) versions.next();
            if (mutation.state.equals(COMMITTED))
            {
                setOfCommitted.add(mutation.version);
            }
        }

        query.insert(record);

        mutator.getJournal().commit();

        mutator = bento.mutate();

        return new Snapshot(mutator, query, setOfCommitted, test, version);
    }

    public Snapshot newSnapshot()
    {
        return newSnapshot(new Test());
    }

    public class Snapshot
    implements BentoStorage.MutatorServer
    {
        private final Bento.Mutator mutator;

        private final Map mapOfBins;

        private final Long version;

        private final Test test;

        private final Long oldest;

        private final Set setOfCommitted;

        public Snapshot(Bento.Mutator mutator, Strata.Query query, Set setOfCommitted, Test test, Long version)
        {
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

        public void commit()
        {
            if (mapOfBins.size() != 0)
            {
                Iterator bags = mapOfBins.values().iterator();
                while (bags.hasNext())
                {
                    Bin bin = (Bin) bags.next();
                    bin.commit();
                }
                mapOfBins.clear();

                test.changesWritten.release();
                try
                {
                    test.registerMutation.acquire();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }

                Strata.Query query = mutations.query(BentoStorage.txn(mutator));

                Mutation.Record committed = new Mutation.Record(version, COMMITTED);
                query.insert(committed);

                mutator.getJournal().commit();

                test.journalComplete.release();
            }
        }

        public void rollback()
        {
            if (mapOfBins.size() != 0)
            {
                Iterator bags = mapOfBins.values().iterator();
                while (bags.hasNext())
                {
                    Bin bin = (Bin) bags.next();
                    bin.rollback();
                }
                mapOfBins.clear();

                Strata.Query query = mutations.query(BentoStorage.txn(mutator));

                Mutation.Record rolledback = new Mutation.Record(version, COMMITTED);
                query.remove(rolledback);

                mutator.getJournal().rollback();
            }
        }

        public Bento.Mutator getMutator()
        {
            return mutator;
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