/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.agtrz.fossil.Fossil;
import com.agtrz.pack.Pack;
import com.agtrz.strata.ArrayListStorage;
import com.agtrz.strata.Strata;

public class Depot
{
    private final static int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;
    private final static int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
    private final static int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
    
    public final static int CORRUPT_FILE_EXCEPTION = 501;

    public final static int CONCURRENT_MODIFICATION_ERROR = 301;

    public final static int UNIQUE_CONSTRAINT_VIOLATION_ERROR = 302;

    public final static int NOT_NULL_VIOLATION_ERROR = 303;

    private final static URI HEADER_URI = URI.create("http://syndibase.agtrz.com/strata");

    private final static Integer OPERATING = new Integer(1);

    private final static Integer COMMITTED = new Integer(2);

    private final Pack pack;

    private final Strata snapshots;

    private final Map<String, Bin.Common> mapOfBinCommons;

    private final Map<String, Bin.Schema> mapOfBinSchemas;

    private final Map<String, Join.Schema> mapOfJoinSchemas;

    private final Sync sync;

    public Depot(File file, Pack pack, Strata mutations, Map<String, Bin.Common> mapOfBinCommons, Map<String, Bin.Schema> mapOfBinSchemas, Map<String, Join.Schema> mapOfJoinSchemas, Sync sync)
    {
        this.mapOfBinCommons = mapOfBinCommons;
        this.mapOfBinSchemas = mapOfBinSchemas;
        this.mapOfJoinSchemas = mapOfJoinSchemas;
        this.snapshots = mutations;
        this.pack = pack;
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

    private static boolean partial(Comparable<?>[] partial, Comparable<?>[] full)
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

    private static int compare(Comparable<?>[] left, Comparable<?>[] right)
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

    private static boolean hasNulls(Comparable<?>[] fields)
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
            Snapshot.Record mutation = (Snapshot.Record) versions.next();
            if (mutation.state.equals(COMMITTED))
            {
                setOfCommitted.add(mutation.version);
            }
        }
        versions.release();

        query.insert(record);

        mutator.commit();

        return new Snapshot(snapshots, mapOfBinCommons, mapOfBinSchemas, mapOfJoinSchemas, mutator, setOfCommitted, test, version, sync);
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

    public final static class Danger
    extends RuntimeException
    {
        private static final long serialVersionUID = 20070210L;

        public final int code;

        public final List<Object> listOfParameters;

        public Danger(String message, Throwable cause, int code)
        {
            super(message, cause);
            this.code = code;
            this.listOfParameters = new ArrayList<Object>();
        }

        public Danger(String message, int code)
        {
            super(message);
            this.code = code;
            this.listOfParameters = new ArrayList<Object>();
        }

        public Danger add(Object parameter)
        {
            listOfParameters.add(parameter);
            return this;
        }
    }

    public final static class Error
    extends RuntimeException
    {
        private static final long serialVersionUID = 20070210L;

        private final Map<String, Object> mapOfProperties;

        public final int code;

        public Error(String message, int code)
        {
            super(message);
            this.code = code;
            this.mapOfProperties = new HashMap<String, Object>();
        }

        public Error(String message, int code, Throwable cause)
        {
            super(message, cause);
            this.code = code;
            this.mapOfProperties = new HashMap<String, Object>();
        }

        public Error put(String name, Object value)
        {
            mapOfProperties.put(name, value);
            return this;
        }

        public Map<String, Object> getProperties()
        {
            return Collections.unmodifiableMap(mapOfProperties);
        }
    }

    public static class Bag
    implements Serializable
    {
        private static final long serialVersionUID = 20070210L;

        private final Long key;

        private final Long version;

        private final Object object;

        public Bag(Long key, Long version, Object object)
        {
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
    }

    // FIXME Vacuum.
    public final static class Bin
    {
        private final String name;

        private final Pack.Mutator mutator;

        private final Snapshot snapshot;

        private final Common common;

        private final Schema schema;

        private final Map<String, Index> mapOfIndices;

        private final Strata.Query query;

        private final Strata.Query isolation;

        public Bin(Snapshot snapshot, Pack.Mutator mutator, String name, Common common, Schema schema, Map<Long, Depot.Janitor> mapOfJanitors)
        {
            Strata.Schema creator = new Strata.Schema();

            creator.setFieldExtractor(new Extractor());
            creator.setMaxDirtyTiers(5);
            creator.setSize(220);

            Fossil.Schema newStorage = new Fossil.Schema();

            newStorage.setReader(new Reader());
            newStorage.setWriter(new Writer());
            newStorage.setSize(SIZEOF_LONG * 2 + Pack.ADDRESS_SIZE);

            creator.setStorage(newStorage);

            Strata isolation = creator.newStrata(Fossil.txn(mutator));

            Bin.Janitor janitor = new Bin.Janitor(isolation, name);

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

            long address = allocation.temporary();
            mapOfJanitors.put(address, janitor);

            this.snapshot = snapshot;
            this.name = name;
            this.common = common;
            this.mapOfIndices = newIndexMap(snapshot, schema);
            this.schema = schema;
            this.query = schema.getStrata().query(Fossil.txn(mutator));
            this.isolation = isolation.query(Fossil.txn(mutator));
            this.mutator = mutator;
        }

        private static Map<String, Index> newIndexMap(Snapshot snapshot, Schema schema)
        {
            Map<String, Index> mapOfIndices = new HashMap<String, Index>();
            Iterator<Map.Entry<String, Index.Schema>> entries = schema.mapOfIndexSchemas.entrySet().iterator();
            while (entries.hasNext())
            {
                Map.Entry<String, Index.Schema> entry = entries.next();
                Index.Schema indexSchema = (Index.Schema) entry.getValue();
                mapOfIndices.put(entry.getKey(), new Index(indexSchema));
            }
            return mapOfIndices;
        }

        public String getName()
        {
            return name;
        }

        public void load(Marshaller marshaller, Iterator<Bag> iterator)
        {
            // FIXME Either check for empty bin or determine if existing
            // commit logic will detect collision.
            while (iterator.hasNext())
            {
                Bag bag = (Bag) iterator.next();

                PackOutputStream allocation = new PackOutputStream(mutator);
                marshaller.marshall(allocation, bag.getObject());

                long address = allocation.allocate();

                Record record = new Record(bag.getKey(), bag.getVersion(), address);

                isolation.insert(record);

                for (Map.Entry<String, Index> entry : mapOfIndices.entrySet())
                {
                    try
                    {
                        ((Index) entry.getValue()).add(snapshot, mutator, this, bag);
                    }
                    catch (Error e)
                    {
                        e.put("index", entry.getKey());
                        isolation.remove(record);
                        mutator.free(record.address);
                        throw e;
                    }
                }
            }
        }

        private void restore(Long key, Object object)
        {
            Bag bag = new Bag(key, snapshot.getVersion(), object);
            insert(bag);
            if (common.identifier <= key.intValue())
            {
                common.identifier = key.intValue() + 1;
            }
        }

        private void insert(Bag bag)
        {
            PackOutputStream allocation = new PackOutputStream(mutator);

            schema.marshaller.marshall(allocation, bag.getObject());

            long address = allocation.allocate();
            Record record = new Record(bag.getKey(), bag.getVersion(), address);
            isolation.insert(record);

            for (Map.Entry<String, Index> entry : mapOfIndices.entrySet())
            {
                try
                {
                    ((Index) entry.getValue()).add(snapshot, mutator, this, bag);
                }
                catch (Error e)
                {
                    e.put("index", entry.getKey());
                    isolation.remove(record);
                    mutator.free(record.address);
                    throw e;
                }
            }
        }

        public Bag add(Object object)
        {
            Bag bag = new Bag(common.nextIdentifier(), snapshot.getVersion(), object);

            insert(bag);

            return bag;
        }

        private static boolean isDeleted(Record record)
        {
            return record.address == Pack.NULL_ADDRESS;
        }

        private Record update(Long key)
        {
            Record record = (Record) isolation.remove(new Comparable[] { key }, Strata.ANY);
            if (record != null)
            {
                mutator.free(record.address);
            }
            else
            {
                record = get(query.find(new Comparable[] { key }), key, false);
            }
            if (record != null)
            {
                record = isDeleted(record) ? null : record;
            }
            return record;
        }

        public Bag update(Long key, Object object)
        {
            Record record = update(key);

            if (record == null)
            {
                throw new Danger("update.bag.does.not.exist", 401);
            }

            Bag bag = new Bag(key, snapshot.getVersion(), object);
            PackOutputStream allocation = new PackOutputStream(mutator);
            schema.marshaller.marshall(allocation, object);
            long address = allocation.allocate();
            isolation.insert(new Record(bag.getKey(), bag.getVersion(), address));

            Iterator<Index> indices = mapOfIndices.values().iterator();
            while (indices.hasNext())
            {
                Index index = (Index) indices.next();
                index.update(snapshot, mutator, this, bag, record.version);
            }

            return bag;
        }

        public void delete(Long key)
        {
            Record record = update(key);

            if (record == null)
            {
                throw new Danger("Deleted record does not exist.", 402);
            }

            if (record.version != snapshot.getVersion())
            {
                isolation.insert(new Record(key, snapshot.getVersion(), Pack.NULL_ADDRESS));
            }
        }

        public void dispose(Record record)
        {

        }

        private Record getVersion(Strata.Cursor cursor, Long key, Long version)
        {
            Record candidate = null;
            while (candidate == null && cursor.hasNext())
            {
                Record record = (Record) cursor.next();
                if (!key.equals(record.key))
                {
                    break;
                }
                if (version.equals(record.version))
                {
                    candidate = record;
                }
            }
            cursor.release();
            return candidate;
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
                if (!key.equals(record.key))
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
            ByteBuffer block = mutator.read(record.address);
            Object object = unmarshaller.unmarshall(new ByteBufferInputStream(block));
            return new Bag(record.key, record.version, object);
        }

        private Record getRecord(Long key)
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

        private Record getRecord(Long key, Long version)
        {
            Record stored = getVersion(query.find(new Comparable[] { key }), key, version);
            Record isolated = getVersion(isolation.find(new Comparable[] { key }), key, version);
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

        public Bag get(Long key)
        {
            return get(schema.unmarshaller, key);
        }

        public Bag get(Unmarshaller unmarshaller, Long key)
        {
            Record record = getRecord(key);
            return record == null ? null : unmarshall(unmarshaller, record);
        }

        Bag get(Unmarshaller unmarshaller, Long key, Long version)
        {
            Record record = getRecord(key, version);
            return record == null ? null : unmarshall(unmarshaller, record);
        }

        // FIXME Call this somewhere somehow.
        void copacetic()
        {
            Set<Long> seen = new HashSet<Long>();
            Strata.Cursor isolated = isolation.first();
            while (isolated.hasNext())
            {
                Record record = (Record) isolated.next();
                if (seen.contains(record))
                {
                    throw new Danger("Duplicate key in isolation.", 0);
                }
                seen.add(record.key);
            }
        }

        private void flush()
        {
            isolation.flush();
        }

        void commit()
        {
            Strata.Cursor isolated = isolation.first();
            while (isolated.hasNext())
            {
                Record record = (Record) isolated.next();
                query.insert(record);
            }
            query.flush();

            boolean copacetic = true;
            isolated = isolation.first();
            while (copacetic && isolated.hasNext())
            {
                Record record = (Record) isolated.next();
                Strata.Cursor cursor = query.find(new Comparable[] { record.key });
                while (copacetic && cursor.hasNext())
                {
                    Record candidate = (Record) cursor.next();
                    
                    assert candidate.key == record.key;

                    if (candidate.version == record.version)
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
                throw new Error("Concurrent modification.", CONCURRENT_MODIFICATION_ERROR);
            }

            Iterator<Index> indices = mapOfIndices.values().iterator();
            while (indices.hasNext())
            {
                Index index = (Index) indices.next();
                index.commit(snapshot, mutator, this);
            }
        }

        public Index.Cursor find(String indexName, Comparable<?>[] fields, boolean limit)
        {
            Index index = (Index) mapOfIndices.get(indexName);
            if (index == null)
            {
                throw new Danger("no.such.index", 503).add(indexName);
            }

            return index.find(snapshot, mutator, this, fields, limit);
        }

        public Index.Cursor find(String string, Comparable<?>[] fields)
        {
            return find(string, fields, true);
        }

        public Index.Cursor first(String indexName)
        {
            Index index = (Index) mapOfIndices.get(indexName);
            if (index == null)
            {
                throw new Danger("no.such.index", 503).add(indexName);
            }

            return index.first(snapshot, mutator, this);
        }

        public Cursor first()
        {
            return first(schema.unmarshaller);
        }

        public Cursor first(Unmarshaller unmarshaller)
        {
            return new Cursor(snapshot, mutator, isolation.first(), schema.getStrata().query(Fossil.txn(mutator)).first(), unmarshaller);
        }
        
        
        private final static class Record
        {
            public final long key;

            public final long version;

            public final long address;

            public Record(long key, long version, long address)
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
                    return key == record.key && version == record.version;
                }
                return false;
            }

            public int hashCode()
            {
                int hashCode = 1;
                hashCode = hashCode * 37 + ((Long) key).hashCode();
                hashCode = hashCode * 37 + ((Long) version).hashCode();
                return hashCode;
            }
        }

        private final static class Extractor
        implements Strata.FieldExtractor, Serializable
        {
            private static final long serialVersionUID = 20070408L;

            public Comparable<?>[] getFields(Object txn, Object object)
            {
                Record record = (Record) object;
                return new Comparable[] { record.key };
            }
        }

        private final static class Writer
        implements Fossil.Writer, Serializable
        {
            private static final long serialVersionUID = 20070208L;

            public void write(ByteBuffer out, Object object)
            {
                Record record = (Record) object;
                out.putLong(record.key);
                out.putLong(record.version);
                out.putLong(record.address);
            }
        }

        private final static class Reader
        implements Fossil.Reader, Serializable
        {
            private static final long serialVersionUID = 20070208L;

            public Object read(ByteBuffer in)
            {
                return new Record(in.getLong(), in.getLong(), in.getLong());
            }
        }

        private final static class Schema
        implements Serializable
        {
            private static final long serialVersionUID = 20070408L;

            public final Object strata;

            public final Map<String, Index.Schema> mapOfIndexSchemas;

            public final Unmarshaller unmarshaller;

            public final Marshaller marshaller;

            public Schema(Strata strata, Map<String, Index.Schema> mapOfIndexSchemas, Unmarshaller unmarshaller, Marshaller marshaller)
            {
                this.strata = strata;
                this.mapOfIndexSchemas = mapOfIndexSchemas;
                this.unmarshaller = unmarshaller;
                this.marshaller = marshaller;
            }

            private Schema(Strata.Schema strata, Map<String, Index.Schema> mapOfIndexSchemas, Unmarshaller unmarshaller, Marshaller marshaller)
            {
                this.strata = strata;
                this.mapOfIndexSchemas = mapOfIndexSchemas;
                this.unmarshaller = unmarshaller;
                this.marshaller = marshaller;
            }

            public Strata getStrata()
            {
                return (Strata) strata;
            }

            private Map<String, Index.Schema> newMapOfIndexStratas(Map<String, Index.Schema> mapOfIndexSchemas, Object txn)
            {
                Map<String, Index.Schema> mapOfIndexStratas = new HashMap<String, Index.Schema>();
                for(Map.Entry<String, Index.Schema> entry : mapOfIndexSchemas.entrySet())
                {
                    String name = entry.getKey();
                    Index.Schema schema = mapOfIndexSchemas.get(name);
                    mapOfIndexStratas.put(name, schema.toStrata(txn));
                }
                return mapOfIndexStratas;
            }

            private Map<String, Index.Schema> newMapOfIndexSchemas(Map<String, Index.Schema> mapOfIndexStratas)
            {
                Map<String, Index.Schema> mapOfIndexSchemas = new HashMap<String, Index.Schema>();
                for(Map.Entry<String, Index.Schema> entry : mapOfIndexStratas.entrySet())
                {
                    String name = entry.getKey();
                    Index.Schema schema = mapOfIndexStratas.get(name);
                    mapOfIndexSchemas.put(name, schema.toSchema());
                }
                return mapOfIndexSchemas;
            }

            public Schema toStrata(Object txn)
            {
                return new Schema(((Strata.Schema) strata).newStrata(txn), newMapOfIndexStratas(mapOfIndexSchemas, txn), unmarshaller, marshaller);
            }

            public Schema toSchema()
            {
                return new Schema(getStrata().getSchema(), newMapOfIndexSchemas(mapOfIndexSchemas), unmarshaller, marshaller);
            }
        }

        public final static class Common
        {
            private long identifier;

            public Common(long identifier)
            {
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

            private final Map<String, Index.Creator> mapOfIndices;

            private Unmarshaller unmarshaller;

            private Marshaller marshaller;

            public Creator(String name)
            {
                this.name = name;
                this.mapOfIndices = new HashMap<String, Index.Creator>();
                this.unmarshaller = new SerializationUnmarshaller();
                this.marshaller = new SerializationMarshaller();
            }

            public String getName()
            {
                return name;
            }

            public Index.Creator newIndex(String name)
            {
                if (mapOfIndices.containsKey(name))
                {
                    throw new IllegalStateException();
                }
                Index.Creator newIndex = new Index.Creator();
                mapOfIndices.put(name, newIndex);
                return newIndex;
            }

            public void setMarshallers(Unmarshaller unmarshaller, Marshaller marshaller)
            {
                this.unmarshaller = unmarshaller;
                this.marshaller = marshaller;
            }
        }

        public final static class Janitor
        implements Depot.Janitor
        {
            private static final long serialVersionUID = 20070826L;

            private final Strata isolation;

            private final String name;

            public Janitor(Strata isolation, String name)
            {
                this.isolation = isolation;
                this.name = name;
            }

            public void rollback(Snapshot snapshot)
            {
                Bin bin = snapshot.getBin(name);
                Strata.Cursor cursor = isolation.query(Fossil.txn(bin.mutator)).first();
                while (cursor.hasNext())
                {
                    Record record = (Record) cursor.next();
                    Iterator<Index> indices = bin.mapOfIndices.values().iterator();
                    while (indices.hasNext())
                    {
                        Index index = (Index) indices.next();
                        index.remove(bin.mutator, bin, record.key, record.version);
                    }
                    bin.query.remove(record);
                }
                cursor.release();
                bin.query.flush();
            }

            public void dispose(Pack.Mutator mutator, boolean deallocate)
            {
                Strata.Query query = isolation.query(Fossil.txn(mutator));
                if (deallocate)
                {
                    Strata.Cursor cursor = query.first();
                    while (cursor.hasNext())
                    {
                        Record record = (Record) cursor.next();
                        mutator.free(record.address);
                    }
                }
                query.destroy();
            }
        }

        public final static class Cursor
        implements Iterator<Bag>
        {
            private final Snapshot snapshot;

            private final Pack.Mutator mutator;

            private final Strata.Cursor isolation;

            private final Strata.Cursor common;

            private final Unmarshaller unmarshaller;

            private Bag nextIsolated;

            private Bag nextCommon;

            private Record[] firstIsolated;

            private Record[] firstCommon;

            public Cursor(Snapshot snapshot, Pack.Mutator mutator, Strata.Cursor isolation, Strata.Cursor common, Unmarshaller unmarshaller)
            {
                this.snapshot = snapshot;
                this.mutator = mutator;
                this.isolation = isolation;
                this.common = common;
                this.unmarshaller = unmarshaller;
                this.firstIsolated = new Record[1];
                this.firstCommon = new Record[1];
                this.nextIsolated = next(isolation, firstIsolated, true);
                this.nextCommon = next(common, firstCommon, false);
            }

            public boolean hasNext()
            {
                return !(nextIsolated == null && nextCommon == null);
            }

            private Bag next(Strata.Cursor cursor, Record[] first, boolean isolated)
            {
                while (first[0] == null && cursor.hasNext())
                {
                    Record record = (Record) cursor.next();
                    if (isolated || snapshot.isVisible(record.version))
                    {
                        first[0] = record;
                    }
                }
                Record candidate;
                do
                {
                    candidate = first[0];
                    for (;;)
                    {
                        if (candidate == null)
                        {
                            break;
                        }
                        if (!cursor.hasNext())
                        {
                            cursor.release();
                            first[0] = null;
                            break;
                        }
                        Record record = (Record) cursor.next();
                        if (first[0].key != record.key)
                        {
                            first[0] = record;
                            break;
                        }
                        if (isolated || snapshot.isVisible(record.version))
                        {
                            candidate = record;
                        }
                    }
                    if (candidate == null)
                    {
                        return null;
                    }
                }
                while (candidate.address == Pack.NULL_ADDRESS);
                ByteBuffer block = mutator.read(candidate.address);
                Object object = unmarshaller.unmarshall(new ByteBufferInputStream(block));
                return new Bag(candidate.key, candidate.version, object);
            }

            public Bag nextBag()
            {
                Bag next = null;
                if (nextIsolated == null)
                {
                    if (nextCommon != null)
                    {
                        next = nextCommon;
                        nextCommon = next(common, firstCommon, false);
                    }
                }
                else if (nextCommon == null)
                {
                    next = nextIsolated;
                    nextIsolated = next(isolation, firstIsolated, true);
                }
                else
                {
                    int compare = nextIsolated.getKey().compareTo(nextCommon.getKey());
                    if (compare == 0)
                    {
                        next = nextIsolated;
                        nextCommon = next(common, firstCommon, false);
                        nextIsolated = next(isolation, firstIsolated, true);
                    }
                    else if (compare < 0)
                    {
                        next = nextIsolated;
                        nextIsolated = next(isolation, firstIsolated, true);
                    }
                    else
                    {
                        next = nextCommon;
                        nextCommon = next(common, firstCommon, false);
                    }
                }
                return next;
            }

            public Bag next()
            {
                return nextBag();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void release()
            {
                isolation.release();
                common.release();
            }
        }
    }

    public interface Janitor
    extends Serializable
    {
        public void rollback(Snapshot snapshot);

        public void dispose(Pack.Mutator mutator, boolean deallocate);
    }

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
            Bin.Schema binSchema = (Bin.Schema) entry.getValue();

            long identifer = 1L;
            Strata.Query query = binSchema.getStrata().query(Fossil.txn(mutator));
            Strata.Cursor last = query.first();
            // FIXME You can use hasPrevious when it is implemented.
            while (last.hasNext())
            {
                Bin.Record record = (Bin.Record) last.next();
                identifer = record.key + 1;
            }
            last.release();

            mapOfBinCommons.put(entry.getKey(), new Bin.Common(identifer));
        }

        return mapOfBinCommons;
    }

    private final static class EmptyDepot
    {
        public final File file;

        public final Pack pack;

        public final Strata snapshots;

        public EmptyDepot(File file)
        {
            Pack.Creator newPack = new Pack.Creator();
            newPack.addStaticPage(HEADER_URI, Pack.ADDRESS_SIZE);
            Pack pack = newPack.create(file);
            Pack.Mutator mutator = pack.mutate();

            Fossil.Schema newMutationStorage = new Fossil.Schema();

            newMutationStorage.setWriter(new Snapshot.Writer());
            newMutationStorage.setReader(new Snapshot.Reader());
            newMutationStorage.setSize(SIZEOF_LONG + SIZEOF_INT);

            Strata.Schema newMutationStrata = new Strata.Schema();

            newMutationStrata.setStorage(newMutationStorage);
            newMutationStrata.setFieldExtractor(new Snapshot.Extractor());
            newMutationStrata.setSize(512);

            Object txn = Fossil.txn(mutator);
            Strata mutations = newMutationStrata.newStrata(txn);

            Strata.Query query = mutations.query(txn);
            query.insert(new Snapshot.Record(new Long(1L), COMMITTED));

            mutator.commit();

            this.file = file;
            this.pack = pack;
            this.snapshots = mutations;
        }
    }

    public final static class Loader
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

    public final static class Creator
    {
        private final Map<String, Bin.Creator> mapOfBinCreators = new HashMap<String, Bin.Creator>();

        private final Map<String, Join.Creator> mapOfJoinCreators = new HashMap<String, Join.Creator>();

        public Bin.Creator newBin(String name)
        {
            if (mapOfBinCreators.containsKey(name))
            {
                throw new IllegalStateException();
            }
            Bin.Creator newBin = new Bin.Creator(name);
            mapOfBinCreators.put(name, newBin);
            return newBin;
        }

        public Join.Creator newJoin(String name)
        {
            if (mapOfJoinCreators.containsKey(name))
            {
                throw new IllegalStateException();
            }
            Join.Creator newJoin = new Join.Creator(mapOfBinCreators.keySet());
            mapOfJoinCreators.put(name, newJoin);
            return newJoin;
        }

        public Depot create(File file)
        {
            return create(file, new NullSync());
        }

        public Depot create(File file, Sync sync)
        {
            Pack.Creator newBento = new Pack.Creator();
            newBento.addStaticPage(HEADER_URI, Pack.ADDRESS_SIZE);
            Pack bento = newBento.create(file);
            Pack.Mutator mutator = bento.mutate();

            Fossil.Schema newMutationStorage = new Fossil.Schema();

            newMutationStorage.setWriter(new Snapshot.Writer());
            newMutationStorage.setReader(new Snapshot.Reader());
            newMutationStorage.setSize(SIZEOF_LONG + SIZEOF_INT);

            Strata.Schema newMutationStrata = new Strata.Schema();

            newMutationStrata.setStorage(newMutationStorage);
            newMutationStrata.setFieldExtractor(new Snapshot.Extractor());
            newMutationStrata.setSize(256);

            Object txn = Fossil.txn(mutator);
            Strata mutations = newMutationStrata.newStrata(txn);

            Strata.Query query = mutations.query(txn);
            query.insert(new Snapshot.Record(new Long(1L), COMMITTED));

            Map<String, Bin.Schema> mapOfBins = new HashMap<String, Bin.Schema>();
            for (Map.Entry<String, Bin.Creator> entry : mapOfBinCreators.entrySet())
            {
                String name = (String) entry.getKey();

                Fossil.Schema newBinStorage = new Fossil.Schema();
                newBinStorage.setWriter(new Bin.Writer());
                newBinStorage.setReader(new Bin.Reader());
                newBinStorage.setSize(SIZEOF_LONG * 2 + Pack.ADDRESS_SIZE);

                Strata.Schema newBinStrata = new Strata.Schema();

                newBinStrata.setStorage(newBinStorage);
                newBinStrata.setFieldExtractor(new Bin.Extractor());
                newBinStrata.setSize(220);
                newBinStrata.setMaxDirtyTiers(1);

                Strata strata = newBinStrata.newStrata(Fossil.txn(mutator));

                Bin.Creator newBin = (Bin.Creator) entry.getValue();

                Map<String, Index.Schema> mapOfIndices = new HashMap<String, Index.Schema>();
                for (Map.Entry<String, Index.Creator> index : newBin.mapOfIndices.entrySet())
                {
                    String nameOfIndex = (String) index.getKey();

                    Fossil.Schema newIndexStorage = new Fossil.Schema();
                    newIndexStorage.setWriter(new Index.Writer());
                    newIndexStorage.setReader(new Index.Reader());
                    newIndexStorage.setSize(SIZEOF_LONG + SIZEOF_LONG + SIZEOF_SHORT);

                    Strata.Schema newIndexStrata = new Strata.Schema();
                    Index.Creator newIndex = (Index.Creator) index.getValue();

                    if (newIndex.unmarshaller == null)
                    {
                        newIndex.setUnmarshaller(newBin.unmarshaller);
                    }

                    newIndexStrata.setStorage(newIndexStorage);
                    newIndexStrata.setFieldExtractor(new Index.Extractor());
                    newIndexStrata.setSize(256);
                    newIndexStrata.setMaxDirtyTiers(1);
                    newIndexStrata.setCacheFields(true);

                    Strata indexStrata = newIndexStrata.newStrata(Fossil.txn(mutator));

                    mapOfIndices.put(nameOfIndex, new Index.Schema(indexStrata, newIndex.extractor, newIndex.unique, newIndex.notNull, newIndex.unmarshaller));
                }

                mapOfBins.put(name, new Bin.Schema(strata, mapOfIndices, newBin.unmarshaller, newBin.marshaller));
            }

            Map<String, Join.Schema> mapOfJoins = new HashMap<String, Join.Schema>();
            for (Map.Entry<String, Join.Creator> join : mapOfJoinCreators.entrySet())
            {
                String joinName = (String) join.getKey();
                Join.Creator newJoin = (Join.Creator) join.getValue();

                Join.Index[] indexes = new Join.Index[newJoin.listOfAlternates.size() + 1];

                String[] order = (String[]) newJoin.mapOfFields.keySet().toArray(new String[newJoin.mapOfFields.size()]);
                indexes[0] = newJoinIndex(mutator, newJoin.mapOfFields, order);

                Map<String, String> mapOfFields = new LinkedHashMap<String, String>(newJoin.mapOfFields);
                for (int i = 0; i < newJoin.listOfAlternates.size(); i++)
                {
                    order = (String[]) newJoin.listOfAlternates.get(i);
                    indexes[i + 1] = newJoinIndex(mutator, mapOfFields, order);
                }
                mapOfJoins.put(joinName, new Join.Schema(indexes, mapOfFields));
            }

            PackOutputStream allocation = new PackOutputStream(mutator);
            try
            {
                ObjectOutputStream out = new ObjectOutputStream(allocation);
                out.writeObject(mutations);
                out.writeObject(mapOfBins);
                out.writeObject(mapOfJoins);
            }
            catch (IOException e)
            {
                throw new Danger("io", e, 0);
            }

            long addressOfBins = allocation.allocate();

            ByteBuffer block = mutator.read(mutator.getStaticPageAddress(HEADER_URI));

            block.putLong(addressOfBins);
            block.flip();

            mutator.write(mutator.getStaticPageAddress(HEADER_URI), block);
            
            mutator.commit();
            bento.close();

            return new Depot.Opener().open(file, sync);
        }
    }

    public final static class Opener
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
            ByteBuffer block = mutator.read(mutator.getStaticPageAddress(HEADER_URI));
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
                Bin.Schema binSchema = (Bin.Schema) entry.getValue();

                long identifer = 1L;
                Strata.Query query = binSchema.getStrata().query(Fossil.txn(mutator));
                Strata.Cursor last = query.first();
                // FIXME You can use hasPrevious when it is implemented.
                while (last.hasNext())
                {
                    Bin.Record record = (Bin.Record) last.next();
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
                Snapshot.Record mutation = (Snapshot.Record) versions.next();
                if (mutation.state.equals(COMMITTED))
                {
                    setOfCommitted.add(mutation.version);
                }
            }
            versions.release();

            Snapshot snapshot = new Snapshot(mutations, mapOfBinCommons, mapOfBinSchemas, mapOfJoinSchemas, pack.mutate(), setOfCommitted, new Test(new NullSync(), new NullSync(), new NullSync()), new Long(0L), new NullSync());
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

            return new Depot(file, pack, mutations, mapOfBinCommons, mapOfBinSchemas, mapOfJoinSchemas, sync);
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

    public interface Marshaller
    {
        public void marshall(OutputStream out, Object object);
    }

    public final static class SerializationMarshaller
    implements Marshaller, Serializable
    {
        private static final long serialVersionUID = 20070826L;

        public void marshall(OutputStream out, Object object)
        {
            try
            {
                new ObjectOutputStream(out).writeObject(object);
            }
            catch (IOException e)
            {
                throw new Danger("io", 403);
            }
        }
    }

    public interface Unmarshaller
    {
        public Object unmarshall(InputStream in);
    }

    public final static class SerializationUnmarshaller
    implements Unmarshaller, Serializable
    {
        private static final long serialVersionUID = 20070826L;

        public Object unmarshall(InputStream in)
        {
            Object object;
            try
            {
                object = new ObjectInputStream(in).readObject();
            }
            catch (IOException e)
            {
                throw new Danger("io", 403);
            }
            catch (ClassNotFoundException e)
            {
                throw new Danger("class.not.found", 403);
            }
            return object;
        }
    }

    public final static class Join
    {
        private final Snapshot snapshot;

        private final Schema schema;

        private final Pack.Mutator mutator;

        private final Strata.Query[] isolation;

        public Join(Snapshot snapshot, Pack.Mutator mutator, Schema schema, String name, Map<Long, Depot.Janitor> mapOfJanitors)
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
                newStorage.setSize(SIZEOF_LONG * schema.mapOfFields.size() + SIZEOF_LONG + SIZEOF_SHORT);

                creator.setStorage(newStorage);

                isolations[i] = creator.newStrata(Fossil.txn(mutator));
            }

            Janitor janitor = new Janitor(isolations, name);

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

        private void flush()
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
            final Record record = new Record(keys, version, deleted);
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
                final Record record = (Record) cursor.next();
                cursor.release();
                if (compare(record.keys, keys) == 0)
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

        public Cursor find(Map<String, Long> mapOfKeys)
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
                    Record record = (Record) isolated.next();
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

        private void commit()
        {
            for (int i = 0; i < schema.indices.length; i++)
            {
                Strata.Query query = schema.indices[i].getStrata().query(Fossil.txn(mutator));
                Strata.Cursor isolated = isolation[i].first();
                while (isolated.hasNext())
                {
                    query.insert((Record) isolated.next());
                }
                isolated.release();
                query.flush();

                boolean copacetic = true;
                isolated = isolation[i].first();
                while (copacetic && isolated.hasNext())
                {
                    Record record = (Record) isolated.next();
                    Strata.Cursor cursor = query.find(record.keys);
                    while (copacetic && cursor.hasNext())
                    {
                        Record candidate = (Record) cursor.next();
                        if (compare(candidate.keys, record.keys) != 0)
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
                    throw new Error("Concurrent modification.", CONCURRENT_MODIFICATION_ERROR);
                }
            }
        }

        public void vacuum()
        {
            for (int i = 0; i < schema.indices.length; i++)
            {
                Strata.Query query = schema.indices[i].getStrata().query(Fossil.txn(mutator));
                Strata.Cursor cursor = query.first();
                Record previous = null;
                while (cursor.hasNext() && previous == null)
                {
                    Record record = (Record) cursor.next();
                    if (snapshot.isVisible(record.version))
                    {
                        previous = record;
                    }
                }
                cursor.release();
                for (;;)
                {
                    cursor = query.find(previous);
                    Record found = null;
                    while (cursor.hasNext() && found == null)
                    {
                        Record record = (Record) cursor.next();
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
                    Record next = null;
                    while (cursor.hasNext() && next == null)
                    {
                        Record record = (Record) cursor.next();
                        if (snapshot.isVisible(record.version))
                        {
                            next = record;
                        }
                    }
                    cursor.release();
                    if (compare(previous.keys, next.keys) == 0 || previous.deleted)
                    {
                        query.remove(previous);
                        query.flush();
                    }
                    previous = next;
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

            public Comparable<?>[] getFields(Object txn, Object object)
            {
                return ((Record) object).keys;
            }
        }

        private final static class Writer
        implements Fossil.Writer, Serializable
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
        implements Fossil.Reader, Serializable
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

            public final Map<String, String> mapOfFields;

            public final Index[] indices;

            public Schema(Index[] indices, Map<String, String> mapOfFields)
            {
                this.indices = indices;
                this.mapOfFields = mapOfFields;
            }

            public Schema toStrata(Object txn)
            {
                Index[] indexes = new Index[this.indices.length];
                for (int i = 0; i < indexes.length; i++)
                {
                    indexes[i] = this.indices[i].toStrata(txn);
                }
                return new Schema(indexes, mapOfFields);
            }

            public Schema toSchema()
            {
                Index[] indexes = new Index[this.indices.length];
                for (int i = 0; i < indexes.length; i++)
                {
                    indexes[i] = this.indices[i].toSchema();
                }
                return new Schema(indexes, mapOfFields);
            }
        }

        private final static class Index
        implements Serializable
        {
            private static final long serialVersionUID = 20070903L;

            public final Object strata;

            public final String[] fields;

            public Index(Strata strata, String[] fields)
            {
                this.strata = strata;
                this.fields = fields;
            }

            private Index(Strata.Schema strata, String[] fields)
            {
                this.strata = strata;
                this.fields = fields;
            }

            public Strata getStrata()
            {
                return (Strata) strata;
            }

            public Index toStrata(Object txn)
            {
                return new Index(((Strata.Schema) strata).newStrata(txn), fields);
            }

            public Index toSchema()
            {
                return new Index(getStrata().getSchema(), fields);
            }
        }

        public final static class Creator
        {
            private final Set<String> setOfBinNames;

            private final Map<String, String> mapOfFields;

            private final List<String[]> listOfAlternates;

            public Creator(Set<String> setOfBinNames)
            {
                this.setOfBinNames = setOfBinNames;
                this.mapOfFields = new LinkedHashMap<String, String>();
                this.listOfAlternates = new ArrayList<String[]>();
            }

            public Creator add(String fieldName, String binName)
            {
                if (!setOfBinNames.contains(binName))
                {
                    throw new IllegalStateException();
                }
                if (mapOfFields.containsKey(fieldName))
                {
                    throw new IllegalStateException();
                }
                mapOfFields.put(fieldName, binName);
                return this;
            }

            public Creator add(String binName)
            {
                return add(binName, binName);
            }

            public void alternate(String[] fields)
            {
                Set<String> seen = new HashSet<String>();
                if (fields.length > mapOfFields.size())
                {
                    throw new IllegalArgumentException();
                }
                for (int i = 0; i < fields.length; i++)
                {
                    if (seen.contains(fields[i]))
                    {
                        throw new IllegalArgumentException();
                    }
                    if (!mapOfFields.containsKey(fields[i]))
                    {
                        throw new IllegalArgumentException();
                    }
                    seen.add(fields[i]);
                }
                listOfAlternates.add(fields);
            }
        }

        private final static class Advancer
        {
            private final Strata.Cursor cursor;

            private Record record;

            private boolean atEnd;

            public Advancer(Strata.Cursor cursor)
            {
                this.cursor = cursor;
            }

            public Record getRecord()
            {
                return record;
            }

            public boolean getAtEnd()
            {
                return atEnd;
            }

            public boolean advance()
            {
                if (!cursor.hasNext())
                {
                    atEnd = true;
                    cursor.release();
                    return false;
                }
                record = (Record) cursor.next();
                return true;
            }

            public void release()
            {
                cursor.release();
            }
        }

        public final static class Cursor
        implements Iterator<Tuple>
        {
            private final Advancer stored;

            private final Advancer isolated;

            private final Snapshot snapshot;

            private final Long[] keys;

            private final Schema schema;

            private Join.Record nextStored;

            private Join.Record nextIsolated;

            private Join.Record next;

            private final Map<String, Long> mapToScan;

            private final Index index;

            public Cursor(Snapshot snapshot, Long[] keys, Map<String, Long> mapToScan, Strata.Cursor storedCursor, Strata.Cursor isolatedCursor, Schema schema, Index index)
            {
                this.snapshot = snapshot;
                this.keys = keys;
                this.stored = new Advancer(storedCursor);
                this.isolated = new Advancer(isolatedCursor);
                this.mapToScan = mapToScan;
                this.index = index;
                this.nextStored = stored.advance() ? next(stored, false) : null;
                this.nextIsolated = isolated.advance() ? next(isolated, true) : null;
                this.next = nextRecord();
                this.schema = schema;
            }

            private Record next(Advancer cursor, boolean isolated)
            {
                Record candidate = null;
                Long[] candidateKeys = null;
                for (;;)
                {
                    if (cursor.getAtEnd())
                    {
                        cursor.release();
                        break;
                    }
                    Record record = cursor.getRecord();
                    if (keys.length > 0 && !partial(keys, record.keys))
                    {
                        cursor.release();
                        break;
                    }
                    if (mapToScan.size() > 0)
                    {
                        for (int i = keys.length; i < index.fields.length; i++)
                        {
                            Long value = (Long) mapToScan.get(index.fields[i]);
                            if (value != null && !record.keys[i].equals(value))
                            {
                                cursor.advance();
                                continue;
                            }
                        }
                    }
                    if (isolated || snapshot.isVisible(record.version))
                    {
                        if (candidateKeys == null)
                        {
                            candidateKeys = record.keys;
                        }
                        else if (!partial(candidateKeys, record.keys))
                        {
                            break;
                        }
                        candidate = record;
                    }
                    cursor.advance();
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

            public Tuple nextTuple()
            {
                Tuple tuple = new Tuple(snapshot, schema.mapOfFields, index.fields, next);
                next = nextRecord();
                return tuple;
            }

            public Tuple next()
            {
                Tuple tuple = new Tuple(snapshot, schema.mapOfFields, index.fields, next);
                next = nextRecord();
                return tuple;
            }

            public void release()
            {
                stored.release();
                isolated.release();
            }
        }

        public final static class Janitor
        implements Depot.Janitor
        {
            private static final long serialVersionUID = 20070826L;

            private final Strata[] isolation;

            private final String name;

            public Janitor(Strata[] isolation, String name)
            {
                this.isolation = isolation;
                this.name = name;
            }

            public void rollback(Snapshot snapshot)
            {
                Join join = snapshot.getJoin(name);
                for (int i = 0; i < join.schema.indices.length; i++)
                {
                    Strata.Query query = join.schema.indices[i].getStrata().query(Fossil.txn(join.mutator));
                    Strata.Cursor cursor = isolation[i].query(Fossil.txn(join.mutator)).first();
                    while (cursor.hasNext())
                    {
                        query.remove((Record) cursor.next());
                    }
                    cursor.release();
                    query.flush();
                }

            }

            public void dispose(Pack.Mutator mutator, boolean deallocate)
            {
                for (int i = 0; i < isolation.length; i++)
                {
                    isolation[i].query(Fossil.txn(mutator)).destroy();
                }
            }
        }
    }
    
    public final static class Pair<A, B>
    {
        int count;

        private A first;
        
        private B second;
        
        public Pair()
        {
        }
        
        @SuppressWarnings("unchecked")
        public void add(Object object)
        {
            if (count == 0)
            {
                first = (A) object;
            }
            else if (count == 1)
            {
                second = (B) object;
            }
            else
            {
                throw new IllegalStateException();
            }
        }
        
        public A getFirst()
        {
            return first;
        }
        
        public B getSecond()
        {
            return second;
        }
    }

    public final static class Trio<A, B, C>
    {
        int count;

        private A first;
        
        private B second;
        
        private C third;
        
        public Trio()
        {
        }
        
        @SuppressWarnings("unchecked")
        public void add(Object object)
        {
            if (count == 0)
            {
                first = (A) object;
            }
            else if (count == 1)
            {
                second = (B) object;
            }
            else if (count == 2)
            {
                third = (C) object;
            }
            else
            {
                throw new IllegalStateException();
            }
        }
        
        public A getFirst()
        {
            return first;
        }
        
        public B getSecond()
        {
            return second;
        }
        
        public C getThird()
        {
            return third;
        }
    }

    public final static class Tuple
    {
        private final Snapshot snapshot;

        private final String[] fields;

        private final Map<String, String> mapOfFields;

        private final Join.Record record;

        public Tuple(Snapshot snapshot, Map<String, String> mapOfFields, String[] fields, Join.Record record)
        {
            this.snapshot = snapshot;
            this.mapOfFields = mapOfFields;
            this.fields = fields;
            this.record = record;
        }

        public Bag getBag(String fieldName)
        {
            for (int i = 0; i < fields.length; i++)
            {
                if (fields[i].equals(fieldName))
                {
                    String bagName = (String) mapOfFields.get(fieldName);
                    return snapshot.getBin(bagName).get(record.keys[i]);
                }
            }
            throw new IllegalArgumentException();
        }

        public Bag getBag(Unmarshaller unmarshaller, String fieldName)
        {
            for (int i = 0; i < fields.length; i++)
            {
                if (fields[i].equals(fieldName))
                {
                    String bagName = (String) mapOfFields.get(fieldName);
                    return snapshot.getBin(bagName).get(unmarshaller, record.keys[i]);
                }
            }
            throw new IllegalArgumentException();
        }

        public Map<String, Long> getKeys()
        {
            Map<String, Long> mapOfKeys = new HashMap<String, Long>();
            for (int i = 0; i < fields.length; i++)
            {
                mapOfKeys.put(fields[i], record.keys[i]);
            }
            return mapOfKeys;
        }
    }

    public interface FieldExtractor
    extends Serializable
    {
        public abstract Comparable<?>[] getFields(Object object);
    }

    public final static class BeanExtractor
    implements FieldExtractor
    {
        private static final long serialVersionUID = 20070917L;

        private final Class<? extends Object> type;

        private final String[] methods;

        public BeanExtractor(Class<? extends Object> type, String[] fields)
        {
            BeanInfo beanInfo;
            try
            {
                beanInfo = Introspector.getBeanInfo(type);
            }
            catch (IntrospectionException e)
            {
                throw new Danger("Extractor", e, 1);
            }
            String[] methods = new String[fields.length];
            PropertyDescriptor[] properties = beanInfo.getPropertyDescriptors();
            for (int i = 0; i < properties.length; i++)
            {
                for (int j = 0; j < fields.length; j++)
                {
                    if (properties[i].getName().equals(fields[j]))
                    {
                        methods[j] = properties[i].getReadMethod().getName();
                    }
                }
            }
            for (int i = 0; i < methods.length; i++)
            {
                if (methods[i] == null)
                {
                    throw new Error("A", 1);
                }
            }
            this.type = type;
            this.methods = methods;
        }

        public Comparable<?>[] getFields(Object object)
        {
            Comparable<?>[] comparables = new Comparable[methods.length];
            for (int i = 0; i < methods.length; i++)
            {
                try
                {
                    comparables[i] = (Comparable<?>) type.getMethod(methods[0], new Class[0]).invoke(object, new Object[0]);
                }
                catch (Exception e)
                {
                    throw new Danger("A.", e, 0);
                }
            }
            return comparables;
        }
    }

    // FIXME Vacuum.
    public final static class Index
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
            Strata.Schema creator = new Strata.Schema();

            creator.setCacheFields(true);
            creator.setFieldExtractor(new Extractor());
            creator.setStorage(new ArrayListStorage.Schema());

            return creator.newStrata(null);
        }

        public void add(Snapshot snapshot, Pack.Mutator mutator, Bin bin, Bag bag)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            Comparable<?>[] fields = schema.extractor.getFields(bag.getObject());
            if (schema.notNull && hasNulls(fields))
            {
                throw new Error("Not null violation.", NOT_NULL_VIOLATION_ERROR);
            }
            if (schema.unique && (schema.notNull || !hasNulls(fields)))
            {
                Iterator<Object> found = find(snapshot, mutator, bin, fields, true);
                if (found.hasNext())
                {
                    found.next(); // Release locks.
                    throw new Error("Unique index constraint violation.", UNIQUE_CONSTRAINT_VIOLATION_ERROR).put("bin", bin.getName());
                }
            }
            Strata.Query query = isolation.query(txn);
            Record record = new Record(bag.getKey(), bag.getVersion());
            query.insert(record);
            query.flush();
        }

        public void update(Snapshot snapshot, Pack.Mutator mutator, Bin bin, Bag bag, Long previous)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            Strata.Query query = isolation.query(txn);
            Strata.Cursor found = query.find(txn.getFields(bag.getKey(), previous));
            while (found.hasNext())
            {
                Record record = (Record) found.next();
                if (record.key.equals(bag.getKey()) && record.version.equals(previous))
                {
                    found.release();
                    query.remove(record);
                    query.flush();
                    break;
                }
            }
            found.release();
            Comparable<?>[] fields = schema.extractor.getFields(bag.getObject());
            if (schema.notNull && hasNulls(fields))
            {
                throw new Error("Not null violation.", NOT_NULL_VIOLATION_ERROR);
            }
            if (schema.unique && (schema.notNull || !hasNulls(fields)))
            {
                Cursor exists = find(snapshot, mutator, bin, fields, true);
                if (exists.hasNext())
                {
                    Bag existing = exists.nextBag();
                    if (!existing.getKey().equals(bag.getKey()))
                    {
                        throw new Error("unique.index.constraint.violation", UNIQUE_CONSTRAINT_VIOLATION_ERROR).put("bin", bin.getName());
                    }
                }
            }
            query.insert(new Record(bag.getKey(), bag.getVersion()));
            query.flush();
        }

        public void remove(Pack.Mutator mutator, Bin bin, Long key, Long version)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            final Bag bag = bin.get(schema.unmarshaller, key, version);
            Comparable<?>[] fields = schema.extractor.getFields(bag.getObject());
            schema.getStrata().query(txn).remove(fields, new Strata.Deletable()
            {
                public boolean deletable(Object object)
                {
                    Record record = (Record) object;
                    return record.key.equals(bag.getKey()) && record.version.equals(bag.getVersion());
                }
            });
        }

        private Cursor find(Snapshot snapshot, Pack.Mutator mutator, Bin bin, Comparable<?>[] fields, boolean limit)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            return new Cursor(schema.getStrata().query(txn).find(fields), isolation.query(txn).find(fields), txn, fields, limit);
        }

        private Cursor first(Snapshot snapshot, Pack.Mutator mutator, Bin bin)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            return new Cursor(schema.getStrata().query(txn).first(), isolation.query(txn).first(), txn, new Comparable[] {}, false);
        }

        private void commit(Snapshot snapshot, Pack.Mutator mutator, Bin bin)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            Strata.Query queryOfIsolated = isolation.query(txn);
            Strata.Query queryOfStored = schema.getStrata().query(txn);
            Strata.Cursor isolated = queryOfIsolated.first();
            try
            {
                while (isolated.hasNext())
                {
                    Record record = (Record) isolated.next();
                    queryOfStored.insert(record);
                    if (schema.unique)
                    {
                        Bag bag = bin.get(schema.unmarshaller, record.key, record.version);
                        Comparable<?>[] fields = schema.extractor.getFields(bag.getObject());
                        if (schema.notNull || !hasNulls(fields))
                        {
                            Strata.Cursor found = queryOfStored.find(fields);
                            try
                            {
                                while (found.hasNext())
                                {
                                    Record existing = (Record) found.next();
                                    if (existing.key.equals(record.key) && existing.version.equals(record.version))
                                    {
                                        break;
                                    }
                                    else if (!snapshot.isVisible(existing.version))
                                    {
                                        throw new Error("Concurrent modification.", CONCURRENT_MODIFICATION_ERROR);
                                    }
                                }
                            }
                            finally
                            {
                                found.release();
                            }
                        }
                    }
                }
            }
            finally
            {
                isolated.release();
                queryOfStored.flush();
            }
        }

        public final static class Creator
        {
            private Unmarshaller unmarshaller;

            private FieldExtractor extractor;

            private boolean unique;

            private boolean notNull;

            public Creator()
            {
            }

            public void setExtractor(Class<? extends Object> type, String field)
            {
                setExtractor(type, new String[] { field });
            }

            public void setExtractor(Class<? extends Object> type, String[] fields)
            {
                setExtractor(new BeanExtractor(type, fields));
            }

            public void setExtractor(FieldExtractor extractor)
            {
                this.extractor = extractor;
            }

            public void setUnmarshaller(Unmarshaller unmarshaller)
            {
                this.unmarshaller = unmarshaller;
            }

            public void setUnique(boolean unique)
            {
                this.unique = unique;
            }

            public void setNotNull(boolean notNull)
            {
                this.notNull = notNull;
            }
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

            public Comparable<?>[] getFields(Object txn, Object object)
            {
                Record record = (Record) object;
                Transaction transaction = (Transaction) txn;
                return transaction.getFields(record.key, record.version);
            }
        }

        public final static class Writer
        implements Fossil.Writer, Serializable
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
        implements Fossil.Reader, Serializable
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

            public final Object strata;

            public final boolean unique;

            public final boolean notNull;

            public final Unmarshaller unmarshaller;

            public Schema(Strata strata, FieldExtractor extractor, boolean unique, boolean notNull, Unmarshaller unmarshaller)
            {
                this.extractor = extractor;
                this.strata = strata;
                this.unique = unique;
                this.notNull = notNull;
                this.unmarshaller = unmarshaller;
            }

            private Schema(Strata.Schema strata, FieldExtractor extractor, boolean unique, boolean notNull, Unmarshaller unmarshaller)
            {
                this.extractor = extractor;
                this.strata = strata;
                this.unique = unique;
                this.notNull = notNull;
                this.unmarshaller = unmarshaller;
            }

            public Strata getStrata()
            {
                return (Strata) strata;
            }

            public Schema toStrata(Object txn)
            {
                return new Schema(((Strata.Schema) strata).newStrata(txn), extractor, unique, notNull, unmarshaller);
            }

            public Schema toSchema()
            {
                return new Schema(getStrata().getSchema(), extractor, unique, notNull, unmarshaller);
            }
        }

        public final static class Transaction
        implements Fossil.MutatorServer
        {
            public final Pack.Mutator mutator;

            public final Bin bin;

            public final Schema schema;

            public Transaction(Pack.Mutator mutator, Bin bin, Schema schema)
            {
                this.mutator = mutator;
                this.bin = bin;
                this.schema = schema;
            }

            public Pack.Mutator getMutator()
            {
                return mutator;
            }

            public Comparable<?>[] getFields(Long key, Long version)
            {
                return schema.extractor.getFields(bin.get(schema.unmarshaller, key, version).getObject());
            }

            public Bag getBag(Record record)
            {
                return bin.get(schema.unmarshaller, record.key);
            }

            public boolean isDeleted(Record record)
            {
                return !record.version.equals(bin.get(schema.unmarshaller, record.key).getVersion());
            }
        }

        public final static class Cursor
        implements Iterator<Object>
        {
            private final Comparable<?>[] fields;

            private final Transaction txn;

            private final Strata.Cursor isolated;

            private final Strata.Cursor stored;

            private final boolean limit;

            private Record nextStored;

            private Record nextIsolated;

            private Bag next;

            public Cursor(Strata.Cursor stored, Strata.Cursor isolated, Transaction txn, Comparable<?>[] fields, boolean limit)
            {
                this.txn = txn;
                this.fields = fields;
                this.limit = limit;
                this.isolated = isolated;
                this.stored = stored;
                this.nextStored = next(stored, false);
                this.nextIsolated = next(isolated, true);
                this.next = seekBag();
            }

            private Record next(Strata.Cursor cursor, boolean isolated)
            {
                while (cursor.hasNext())
                {
                    Record record = (Record) cursor.next();
                    Bag bag = txn.bin.get(txn.schema.unmarshaller, record.key);
                    if (bag == null || !bag.getVersion().equals(record.version))
                    {
                        continue;
                    }
                    if (limit && !partial(fields, txn.schema.extractor.getFields(bag.getObject())))
                    {
                        cursor.release();
                        return null;
                    }
                    return record;
                }
                cursor.release();
                return null;
            }

            private Bag seekBag()
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

            public Bag nextBag()
            {
                Bag bag = next;
                next = seekBag();
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
                return nextBag().getObject();
            }

            public void release()
            {
                isolated.release();
                stored.release();
            }
        }

        public final static class Range
        implements Iterator<Object>
        {
            private int count;

            private final int limit;

            private final Cursor cursor;

            public Range(Cursor cursor, int offset, int limit)
            {
                while (offset != 0 && cursor.hasNext())
                {
                    cursor.next();
                    offset--;
                }
                this.limit = limit;
                this.cursor = cursor;
            }

            public boolean hasNext()
            {
                return count < limit && cursor.hasNext();
            }

            public Depot.Bag nextBag()
            {
                Depot.Bag bag = cursor.nextBag();
                count++;
                return bag;
            }

            public Object next()
            {
                Depot.Bag bag = nextBag();
                if (!hasNext())
                {
                    cursor.release();
                }
                return bag.getObject();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void release()
            {
                cursor.release();
            }
        }

    }

    public final static class Restoration
    {
        public final static class Schema
        implements Serializable
        {
            private static final long serialVersionUID = 20071025L;

            private final Map<String, Bin.Schema> mapOfBinSchemas;

            private final Map<String, Depot.Join.Schema> mapOfJoinSchemas;

            public Schema(Map<String, Bin.Schema> mapOfBinSchemas, Map<String, Depot.Join.Schema> mapOfJoinSchemas)
            {
                this.mapOfBinSchemas = new HashMap<String, Bin.Schema>(mapOfBinSchemas);
                this.mapOfJoinSchemas = new HashMap<String, Depot.Join.Schema>(mapOfJoinSchemas);

                Iterator<String> bins = new HashSet<String>(this.mapOfBinSchemas.keySet()).iterator();
                while (bins.hasNext())
                {
                    String name = (String) bins.next();
                    Bin.Schema schema = (Bin.Schema) this.mapOfBinSchemas.get(name);
                    this.mapOfBinSchemas.put(name, schema.toSchema());
                }
                Iterator<String> joins = new HashSet<String>(this.mapOfJoinSchemas.keySet()).iterator();
                while (joins.hasNext())
                {
                    String name = (String) joins.next();
                    Depot.Join.Schema schema = (Depot.Join.Schema) this.mapOfJoinSchemas.get(name);
                    this.mapOfJoinSchemas.put(name, schema.toSchema());
                }
            }

            public Depot newDepot(File file, Sync sync)
            {
                EmptyDepot empty = new EmptyDepot(file);

                Pack.Mutator mutator = empty.pack.mutate();
                Object txn = Fossil.txn(mutator);

                Iterator<String> bins = new HashSet<String>(mapOfBinSchemas.keySet()).iterator();
                while (bins.hasNext())
                {
                    String name = (String) bins.next();
                    Bin.Schema schema = (Bin.Schema) mapOfBinSchemas.get(name);
                    mapOfBinSchemas.put(name, schema.toStrata(txn));
                }

                Iterator<String> joins = new HashSet<String>(mapOfJoinSchemas.keySet()).iterator();
                while (joins.hasNext())
                {
                    String name = (String) joins.next();
                    Depot.Join.Schema schema = (Depot.Join.Schema) mapOfJoinSchemas.get(name);
                    mapOfJoinSchemas.put(name, schema.toStrata(txn));
                }

                Map<String, Bin.Common> mapOfBinCommons = newMapOfBinCommons(mapOfBinSchemas, mutator);
                return new Depot(empty.file, empty.pack, empty.snapshots, mapOfBinCommons, mapOfBinSchemas, mapOfJoinSchemas, sync);
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

    public final static class Snapshot
    {
        private final Strata snapshots;

        private final Map<String, Bin.Common> mapOfBinCommons;

        private final Map<String, Bin.Schema> mapOfBinSchemas;

        private final Map<String, Join.Schema> mapOfJoinSchemas;

        private final Map<Long, Depot.Janitor> mapOfJanitors;

        private final Set<Long> setOfCommitted;

        private final Pack.Mutator mutator;

        private final Map<String, Bin> mapOfBins;

        private final Map<String, Join> mapOfJoins;

        private final Long version;

        private final Test test;

        private final Long oldest;

        private boolean spent;

        private final Sync sync;

        public Snapshot(Strata snapshots, Map<String, Bin.Common> mapOfBinCommons, Map<String, Bin.Schema> mapOfBinSchemas, Map<String, Join.Schema> mapOfJoinSchemas, Pack.Mutator mutator, Set<Long> setOfCommitted, Test test, Long version, Sync sync)
        {
            this.snapshots = snapshots;
            this.mapOfBinCommons = mapOfBinCommons;
            this.mapOfBinSchemas = mapOfBinSchemas;
            this.mapOfJoinSchemas = mapOfJoinSchemas;
            this.mutator = mutator;
            this.mapOfBins = new HashMap<String, Bin>();
            this.mapOfJoins = new HashMap<String, Join>();
            this.version = version;
            this.test = test;
            this.setOfCommitted = setOfCommitted;
            this.oldest = (Long) setOfCommitted.iterator().next();
            this.mapOfJanitors = new HashMap<Long, Janitor>();
            this.sync = sync;
        }

        // FIXME Why name bins? Why not just store objects?
        public Bin getBin(String name)
        {
            Bin bin = (Bin) mapOfBins.get(name);
            if (bin == null)
            {
                Bin.Common binCommon = (Bin.Common) mapOfBinCommons.get(name);
                Bin.Schema binSchema = (Bin.Schema) mapOfBinSchemas.get(name);
                // FIXME Why a new mutator every time? WRONG!
                bin = new Bin(this, mutator, name, binCommon, binSchema, mapOfJanitors);
                mapOfBins.put(name, bin);
            }
            return bin;
        }

        public Join getJoin(String joinName)
        {
            Join join = (Join) mapOfJoins.get(joinName);
            if (join == null)
            {
                Join.Schema schema = (Join.Schema) mapOfJoinSchemas.get(joinName);
                join = new Join(this, mutator, schema, joinName, mapOfJanitors);
                mapOfJoins.put(joinName, join);
            }
            return join;
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

        public void dump(ObjectOutputStream out) throws IOException
        {
            out.writeObject(new Restoration.Schema(mapOfBinSchemas, mapOfJoinSchemas));

            for (String name : mapOfBinSchemas.keySet())
            {
                Bin.Cursor bags = getBin(name).first();
                while (bags.hasNext())
                {
                    Bag bag = bags.nextBag();
                    out.writeObject(new Restoration.Bag(name, bag.getKey(), bag.getObject()));
                }
            }

            for (String name : mapOfJoinSchemas.keySet())
            {
                Join.Cursor links = getJoin(name).find(new HashMap<String, Long>());
                while (links.hasNext())
                {
                    Tuple tuple = (Tuple) links.nextTuple();
                    out.writeObject(new Restoration.Join(name, tuple.getKeys()));
                }
            }
        }

        public void commit()
        {
            if (spent)
            {
                throw new Danger("commit.spent.snapshot", 501);
            }

            spent = true;

            for (Bin bin : mapOfBins.values())
            {
                bin.flush();
            }

            for (Join join : mapOfJoins.values())
            {
                join.flush();
            }

            try
            {
                for (Bin bin : mapOfBins.values())
                {
                    bin.commit();
                }

                for (Join join : mapOfJoins.values())
                {
                    join.commit();
                }
            }
            catch (Error e)
            {
                test.changesWritten();

                for (Map.Entry<Long, Janitor> entry : mapOfJanitors.entrySet())
                {
                    entry.getValue().rollback(this);

                    mutator.free(entry.getKey());
                    entry.getValue().dispose(mutator, true);
                }

                mutator.commit();

                Strata.Query query = snapshots.query(Fossil.txn(mutator));
                query.remove(new Comparable[] { version }, Strata.ANY);

                test.journalComplete.release();

                throw e;
            }

            test.changesWritten();

            for (Map.Entry<Long, Janitor> entry : mapOfJanitors.entrySet())
            {
                mutator.free(entry.getKey());
                entry.getValue().dispose(mutator, false);
            }

            Strata.Query query = snapshots.query(Fossil.txn(mutator));

            Record committed = new Record(version, COMMITTED);
            query.insert(committed);

            query.remove(new Comparable[] { version }, Strata.ANY);

            test.journalComplete.release();

            sync.release();
        }

        public void rollback()
        {
            // FIXME Rethink. Cannot reuse.
            if (!spent)
            {
                spent = true;
                mutator.commit();
                for (Map.Entry<Long, Janitor> entry : mapOfJanitors.entrySet())
                {
                    mutator.free(entry.getKey());
                    entry.getValue().dispose(mutator, true);
                }

                Strata.Query query = snapshots.query(Fossil.txn(mutator));

                query.remove(new Comparable[] { version }, Strata.ANY);

                sync.release();
            }
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

            public Comparable<?>[] getFields(Object txn, Object object)
            {
                Record record = (Record) object;
                return new Comparable[] { record.version };
            }
        }

        private final static class Writer
        implements Fossil.Writer, Serializable
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
        implements Fossil.Reader, Serializable
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

    public final static Test newTest()
    {
        return new Test(new Latch(), new Latch(), new Latch());
    }
    
    public interface Sync
    {
        public void acquire();
        
        public void release();
    }
    
    public final static class NullSync
    implements Sync
    {
        public void acquire()
        {
        }
        
        public void release()
        {
        }
    }
    
    public final static class Latch
    implements Sync
    {
        private boolean unlatched;
        
        public synchronized void acquire()
        {
            while (!unlatched)
            {
                try
                {
                    wait();
                }
                catch (InterruptedException e)
                {
                }
            }
        }
        
        public void release()
        {
            if (!unlatched)
            {
                unlatched = true;
                notifyAll();
            }
        }
    }

    public final static class Test
    {
        private Sync registerMutation;

        private Sync changesWritten;

        private Sync journalComplete;

        private Test(Sync changesWritten, Sync registerMutation, Sync journalComplete)
        {
            this.changesWritten = changesWritten;
            this.registerMutation = registerMutation;
            this.journalComplete = journalComplete;
        }

        private void changesWritten()
        {
            changesWritten.release();
            registerMutation.acquire();
        }

        public void waitForChangesWritten()
        {
            changesWritten.acquire();
        }

        public void registerMutation()
        {
            registerMutation.release();
        }

        public void waitForCompletion()
        {
            journalComplete.acquire();
        }
    }
    
    private final static class PackOutputStream
    extends ByteArrayOutputStream
    {
        private final Pack.Mutator mutator;

        public PackOutputStream(Pack.Mutator mutator)
        {
            this.mutator = mutator;
        }

        private void write(long address)
        {
            ByteBuffer bytes = ByteBuffer.allocateDirect(size());
            bytes.put(toByteArray());
            bytes.flip();

            mutator.write(address, bytes);
        }

        public long allocate()
        {
            long address = mutator.allocate(size());
            write(address);
            return address;
        }

        public long temporary()
        {
            long address = mutator.temporary(size());
            write(address);
            return address;
        }
    }

    public final static class ByteBufferInputStream
    extends InputStream
    {
        private final ByteBuffer bytes;

        public ByteBufferInputStream(ByteBuffer bytes)
        {
            this.bytes = bytes;
        }

        public int read() throws IOException
        {
            if (!bytes.hasRemaining())
            {
                return -1;
            }
            return bytes.get() & 0xff;
        }

        public int read(byte[] b, int off, int len) throws IOException
        {
            len = Math.min(len, bytes.remaining());
            if (len == 0)
            {
                return -1;
            }
            bytes.get(b, off, len);
            return len;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */