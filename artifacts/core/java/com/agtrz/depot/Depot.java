/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
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

import EDU.oswego.cs.dl.util.concurrent.Latch;
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
    public final static int CORRUPT_FILE_EXCEPTION = 501;

    public final static int CONCURRENT_MODIFICATION_ERROR = 301;

    public final static int UNIQUE_CONSTRAINT_VIOLATION_ERROR = 302;

    public final static int NOT_NULL_VIOLATION_ERROR = 303;

    private final static URI HEADER_URI = URI.create("http://syndibase.agtrz.com/strata");

    private final static Integer OPERATING = new Integer(1);

    private final static Integer COMMITTED = new Integer(2);

    private final Bento bento;

    private final Strata snapshots;

    private final Map mapOfBinCommons;

    private final Map mapOfJoinSchemas;

    public Depot(File file, Bento bento, Strata mutations, Map mapOfBinCommons, Map mapOfJoinSchemas)
    {
        this.mapOfBinCommons = mapOfBinCommons;
        this.mapOfJoinSchemas = mapOfJoinSchemas;
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

    private static boolean hasNulls(Comparable[] fields)
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

        return new Snapshot(snapshots, mapOfBinCommons, mapOfJoinSchemas, bento, setOfCommitted, test, version);
    }

    public Snapshot newSnapshot()
    {
        return newSnapshot(new Test(new NullSync(), new NullSync(), new NullSync()));
    }

    public final static class Danger
    extends RuntimeException
    {
        private static final long serialVersionUID = 20070210L;

        public final int code;

        public final List listOfParameters;

        public Danger(String message, Throwable cause, int code)
        {
            super(message, cause);
            this.code = code;
            this.listOfParameters = new ArrayList();
        }

        public Danger(String message, int code)
        {
            super(message);
            this.code = code;
            this.listOfParameters = new ArrayList();
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

        private final Map mapOfProperties;

        public final int code;

        public Error(String message, int code)
        {
            super(message);
            this.code = code;
            this.mapOfProperties = new HashMap();
        }

        public Error put(String name, Object value)
        {
            mapOfProperties.put(name, value);
            return this;
        }

        public Map getProperties()
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

    public final static class Bin
    {
        private final String name;

        private final Bento.Mutator mutator;

        private final Snapshot snapshot;

        private final Common common;

        private final Map mapOfIndices;

        private final Strata.Query query;

        private final Strata.Query isolation;

        public Bin(Snapshot snapshot, Bento.Mutator mutator, String name, Common common, Map mapOfJanitors)
        {
            Strata.Creator creator = new Strata.Creator();

            creator.setFieldExtractor(new Extractor());
            creator.setMaxDirtyTiers(5);
            creator.setSize(512);

            BentoStorage.Creator newStorage = new BentoStorage.Creator();

            newStorage.setReader(new Reader());
            newStorage.setWriter(new Writer());
            newStorage.setSize(SizeOf.LONG * 3 + SizeOf.INTEGER);

            creator.setStorage(newStorage.create());

            Strata isolation = creator.create(BentoStorage.txn(mutator));

            Bin.Janitor janitor = new Bin.Janitor(isolation, name);

            Bento.OutputStream allocation = new Bento.OutputStream(mutator);
            try
            {
                ObjectOutputStream out = new ObjectOutputStream(allocation);
                out.writeObject(janitor);
            }
            catch (IOException e)
            {
                throw new Danger("Cannot write output stream.", e, 0);
            }

            mapOfJanitors.put(allocation.temporary(true), janitor);

            this.snapshot = snapshot;
            this.name = name;
            this.common = common;
            this.mapOfIndices = newIndexMap(snapshot, common);
            this.query = common.schema.strata.query(BentoStorage.txn(mutator));
            this.isolation = isolation.query(BentoStorage.txn(mutator));
            this.mutator = mutator;
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

        public Bag add(Marshaller marshaller, Object object)
        {
            Bag bag = new Bag(common.nextIdentifier(), snapshot.getVersion(), object);

            Bento.OutputStream allocation = new Bento.OutputStream(mutator);

            marshaller.marshall(allocation, object);

            Bento.Address address = allocation.allocate(false);
            Record record = new Record(bag.getKey(), bag.getVersion(), address);

            isolation.insert(record);

            Iterator indices = mapOfIndices.values().iterator();
            while (indices.hasNext())
            {
                Index index = (Index) indices.next();
                try
                {
                    index.add(snapshot, mutator, this, bag);
                }
                catch (Error e)
                {
                    isolation.remove(record);
                    mutator.free(mutator.load(record.address));
                    throw e;
                }
            }

            return bag;
        }

        private static boolean isDeleted(Record record)
        {
            return record.address.equals(Bento.NULL_ADDRESS);
        }

        private Record update(Long key)
        {
            Record record = (Record) isolation.remove(new Comparable[] { key }, Strata.ANY);
            if (record != null)
            {
                mutator.free(mutator.load(record.address));
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

        public Bag update(Marshaller marshaller, Long key, Object object)
        {
            Record record = update(key);

            if (record == null)
            {
                throw new Danger("update.bag.does.not.exist", 401);
            }

            Bag bag = new Bag(key, snapshot.getVersion(), object);
            Bento.OutputStream allocation = new Bento.OutputStream(mutator);
            marshaller.marshall(allocation, object);
            Bento.Address address = allocation.allocate(false);
            isolation.insert(new Record(bag.getKey(), bag.getVersion(), address));

            Iterator indices = mapOfIndices.values().iterator();
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

            if (!record.version.equals(snapshot.getVersion()))
            {
                isolation.insert(new Record(key, snapshot.getVersion(), Bento.NULL_ADDRESS));
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
            Bento.Block block = mutator.load(record.address);
            Object object = unmarshaller.unmarshall(new ByteBufferInputStream(block.toByteBuffer(), false));
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
            Set seen = new HashSet();
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
                    assert candidate.key.equals(record.key);
                    if (candidate.version.equals(record.version))
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

            Iterator indices = mapOfIndices.values().iterator();
            while (indices.hasNext())
            {
                Index index = (Index) indices.next();
                index.commit(snapshot, mutator, this);
            }
        }

        public Index.Cursor find(String indexName, Comparable[] fields, boolean limit)
        {
            Index index = (Index) mapOfIndices.get(indexName);
            if (index == null)
            {
                throw new Danger("no.such.index", 503).add(indexName);
            }

            return index.find(snapshot, mutator, this, fields, limit);
        }

        public Index.Cursor find(String string, Comparable[] fields)
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

        public Cursor first(Unmarshaller unmarshaller)
        {
            return new Cursor(snapshot, mutator, isolation.first(), common.schema.strata.query(BentoStorage.txn(mutator)).first(), unmarshaller);
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
                return new Comparable[] { record.key };
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

            public final Map mapOfIndexSchemas;

            public Schema(Strata strata, Map mapOfIndexSchemas, Unmarshaller unmarshaller, Marshaller marshaller)
            {
                this.strata = strata;
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

            private Unmarshaller unmarshaller;

            private Marshaller marshaller;

            public Creator(String name)
            {
                this.name = name;
                this.mapOfIndices = new HashMap();
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
                Strata.Cursor cursor = isolation.query(BentoStorage.txn(bin.mutator)).first();
                while (cursor.hasNext())
                {
                    Record record = (Record) cursor.next();
                    Iterator indices = bin.mapOfIndices.values().iterator();
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

            public void dispose(Bento.Mutator mutator, boolean deallocate)
            {
                Strata.Query query = isolation.query(BentoStorage.txn(mutator));
                if (deallocate)
                {
                    Strata.Cursor cursor = query.first();
                    while (cursor.hasNext())
                    {
                        Record record = (Record) cursor.next();
                        mutator.free(mutator.load(record.address));
                    }
                }
                query.destroy();
            }
        }

        public final static class Cursor
        implements Iterator
        {
            private final Snapshot snapshot;

            private final Bento.Mutator mutator;

            private final Strata.Cursor isolation;

            private final Strata.Cursor common;

            private final Unmarshaller unmarshaller;

            private Bag nextIsolated;

            private Bag nextCommon;

            private Record[] firstIsolated;

            private Record[] firstCommon;

            public Cursor(Snapshot snapshot, Bento.Mutator mutator, Strata.Cursor isolation, Strata.Cursor common, Unmarshaller unmarshaller)
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
                Record candidate = first[0];
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
                    if (!first[0].key.equals(record.key))
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
                Bento.Block block = mutator.load(candidate.address);
                Object object = unmarshaller.unmarshall(new ByteBufferInputStream(block.toByteBuffer(), false));
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

            public Object next()
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

        public void dispose(Bento.Mutator mutator, boolean deallocate);
    }

    private static Strata newJoinStrata(Bento.Mutator mutator, int size)
    {

        BentoStorage.Creator newJoinStorage = new BentoStorage.Creator();
        newJoinStorage.setWriter(new Join.Writer(size));
        newJoinStorage.setReader(new Join.Reader(size));
        newJoinStorage.setSize(SizeOf.LONG * size + SizeOf.LONG + SizeOf.SHORT);

        Strata.Creator newJoinStrata = new Strata.Creator();

        newJoinStrata.setStorage(newJoinStorage.create());
        newJoinStrata.setFieldExtractor(new Join.Extractor());
        newJoinStrata.setSize(512);

        return newJoinStrata.create(BentoStorage.txn(mutator));
    }

    private static Join.Index newJoinIndex(Bento.Mutator mutator, Map mapOfFields, String[] order)
    {
        List listOfFields = new ArrayList();
        for (int i = 0; i < order.length; i++)
        {
            listOfFields.add(order[i]);
        }
        if (order.length < mapOfFields.size())
        {
            Iterator fields = mapOfFields.keySet().iterator();
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

    public final static class Creator
    {
        private final Map mapOfBinCreators = new HashMap();

        private final Map mapOfJoinCreators = new HashMap();

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
            newMutationStrata.setSize(512);

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
                newBinStrata.setSize(512);

                Strata strata = newBinStrata.create(BentoStorage.txn(mutator));

                Bin.Creator newBin = (Bin.Creator) entry.getValue();

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

                    Strata.Creator newIndexStrata = new Strata.Creator();
                    Index.Creator newIndex = (Index.Creator) index.getValue();

                    newIndexStrata.setStorage(newIndexStorage.create());
                    newIndexStrata.setFieldExtractor(new Index.Extractor());
                    newIndexStrata.setSize(512);
                    newIndexStrata.setCacheFields(true);

                    Strata indexStrata = newIndexStrata.create(BentoStorage.txn(mutator));

                    mapOfIndices.put(nameOfIndex, new Index.Schema(indexStrata, newIndex.extractor, newIndex.unique, newIndex.notNull, newIndex.unmarshaller));
                }

                mapOfBins.put(name, new Bin.Schema(strata, mapOfIndices, newBin.unmarshaller, newBin.marshaller));
            }

            Map mapOfJoins = new HashMap();
            Iterator joins = mapOfJoinCreators.entrySet().iterator();
            while (joins.hasNext())
            {
                Map.Entry join = (Map.Entry) joins.next();

                String joinName = (String) join.getKey();
                Join.Creator newJoin = (Join.Creator) join.getValue();

                Join.Index[] indexes = new Join.Index[newJoin.listOfAlternates.size() + 1];

                String[] order = (String[]) newJoin.mapOfFields.keySet().toArray(new String[newJoin.mapOfFields.size()]);
                indexes[0] = newJoinIndex(mutator, newJoin.mapOfFields, order);

                Map mapOfFields = new LinkedHashMap(newJoin.mapOfFields);
                for (int i = 0; i < newJoin.listOfAlternates.size(); i++)
                {
                    order = (String[]) newJoin.listOfAlternates.get(i);
                    indexes[i + 1] = newJoinIndex(mutator, mapOfFields, order);
                }
                mapOfJoins.put(joinName, new Join.Schema(indexes, mapOfFields));
            }

            Bento.OutputStream allocation = new Bento.OutputStream(mutator);
            try
            {
                ObjectOutputStream out = new ObjectOutputStream(allocation);
                out.writeObject(mutations);
                out.writeObject(mapOfBins);
                out.writeObject(mapOfJoins);
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
            Bento.Opener opener = new Bento.Opener(file);
            Bento bento = opener.open();
            Bento.Mutator mutator = bento.mutate();
            Bento.Block block = mutator.load(bento.getStaticAddress(HEADER_URI));
            ByteBuffer data = block.toByteBuffer();
            Bento.Address addressOfBags = new Bento.Address(data.getLong(), data.getInt());
            Strata mutations = null;
            Map mapOfBinSchemas = null;
            Map mapOfJoinSchemas = null;
            try
            {
                ObjectInputStream objects = new ObjectInputStream(new ByteBufferInputStream(mutator.load(addressOfBags).toByteBuffer(), false));
                mutations = (Strata) objects.readObject();
                mapOfBinSchemas = (Map) objects.readObject();
                mapOfJoinSchemas = (Map) objects.readObject();
            }
            catch (IOException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }
            catch (ClassNotFoundException e)
            {
                // FIXME Entirely possible and common.
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }
            mutator.getJournal().commit();

            Map mapOfBinCommons = new HashMap();
            Iterator bins = mapOfBinSchemas.entrySet().iterator();
            while (bins.hasNext())
            {
                Map.Entry entry = (Map.Entry) bins.next();
                Bin.Schema binSchema = (Bin.Schema) entry.getValue();

                long identifer = 1L;
                Strata.Query query = binSchema.strata.query(BentoStorage.txn(mutator));
                Strata.Cursor last = query.first();
                // FIXME You can use hasPrevious when it is implemented.
                while (last.hasNext())
                {
                    Bin.Record record = (Bin.Record) last.next();
                    identifer = record.key.longValue() + 1;
                }
                last.release();

                mapOfBinCommons.put(entry.getKey(), new Bin.Common(binSchema, identifer));
            }
            Strata.Query query = mutations.query(BentoStorage.txn(mutator));

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

            Snapshot snapshot = new Snapshot(mutations, mapOfBinCommons, mapOfJoinSchemas, bento, setOfCommitted, new Test(new NullSync(), new NullSync(), new NullSync()), new Long(0L));
            Iterator failures = opener.getTemporaryBlocks().iterator();
            while (failures.hasNext())
            {
                Bento.Address address = (Bento.Address) failures.next();
                mutator = bento.mutate();
                block = mutator.load(address);
                Janitor janitor = null;
                try
                {
                    ObjectInputStream in = new ObjectInputStream(new ByteBufferInputStream(block.toByteBuffer(), true));
                    janitor = (Janitor) in.readObject();
                }
                catch (Exception e)
                {
                    throw new Danger("Cannot reopen journal.", e, 0);
                }
                janitor.rollback(snapshot);
                mutator.getJournal().commit();

                mutator.free(mutator.load(address));
                janitor.dispose(mutator, true);
                mutator.getJournal().commit();
            }

            return new Depot(file, bento, mutations, mapOfBinCommons, mapOfJoinSchemas);
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
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
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
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
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

        private final Bento.Mutator mutator;

        private final Strata.Query[] isolation;

        public Join(Snapshot snapshot, Bento.Mutator mutator, Schema schema, String name, Map mapOfJanitors)
        {
            Strata[] isolations = new Strata[schema.indices.length];

            for (int i = 0; i < schema.indices.length; i++)
            {
                Strata.Creator creator = new Strata.Creator();

                creator.setFieldExtractor(new Extractor());
                creator.setMaxDirtyTiers(5);
                creator.setSize(512);

                BentoStorage.Creator newStorage = new BentoStorage.Creator();

                newStorage.setReader(new Reader(schema.mapOfFields.size()));
                newStorage.setWriter(new Writer(schema.mapOfFields.size()));
                newStorage.setSize(SizeOf.LONG * schema.mapOfFields.size() + SizeOf.LONG + SizeOf.SHORT);

                creator.setStorage(newStorage.create());

                isolations[i] = creator.create(BentoStorage.txn(mutator));
            }

            Janitor janitor = new Janitor(isolations, name);

            Bento.OutputStream allocation = new Bento.OutputStream(mutator);
            try
            {
                ObjectOutputStream out = new ObjectOutputStream(allocation);
                out.writeObject(janitor);
            }
            catch (IOException e)
            {
                throw new Danger("Cannot write output stream.", e, 0);
            }

            mapOfJanitors.put(allocation.temporary(true), janitor);

            Strata.Query[] isolationQueries = new Strata.Query[isolations.length];
            for (int i = 0; i < isolations.length; i++)
            {
                isolationQueries[i] = isolations[i].query(BentoStorage.txn(mutator));
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

        public void unlink(Map mapOfKeys)
        {
            add(mapOfKeys, snapshot.getVersion(), true);
        }

        public void link(Map mapOfKeys)
        {
            add(mapOfKeys, snapshot.getVersion(), false);
        }

        private void insertIsolation(Map mapOfKeys, Long version, boolean deleted, int index)
        {
            Long[] keys = new Long[schema.mapOfFields.size()];
            for (int i = 0; i < keys.length; i++)
            {
                keys[i] = (Long) mapOfKeys.get(schema.indices[index].fields[i]);
            }
            isolation[index].insert(new Record(keys, version, deleted));
        }

        private void removeIsolation(Map mapOfKeys, Long version, boolean deleted, int index)
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

        private void add(Map mapOfKeys, Long version, boolean deleted)
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

        public Cursor find(Map mapOfKeys)
        {
            if (mapOfKeys.size() == 0)
            {
                throw new IllegalArgumentException();
            }
            Iterator fields = mapOfKeys.keySet().iterator();
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

            Strata.Query common = schema.indices[index].strata.query(BentoStorage.txn(mutator));
            Long[] keys = new Long[most];
            if (most == 0)
            {
                return new Cursor(snapshot, keys, mapOfKeys, common.first(), isolation[index].first(), schema, schema.indices[index]);
            }

            Map mapToScan = new HashMap(mapOfKeys);
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
                Set seen = new HashSet();
                Strata.Cursor isolated = isolation[i].first();
                while (isolated.hasNext())
                {
                    Record record = (Record) isolated.next();
                    List listOfKeys = new ArrayList();
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
                Strata.Query query = schema.indices[i].strata.query(BentoStorage.txn(mutator));
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
                Strata.Query query = schema.indices[i].strata.query(BentoStorage.txn(mutator));
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

            public Comparable[] getFields(Object txn, Object object)
            {
                return ((Record) object).keys;
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

            public final Index[] indices;

            public Schema(Index[] indices, Map mapOfFields)
            {
                this.indices = indices;
                this.mapOfFields = mapOfFields;
            }
        }

        private final static class Index
        implements Serializable
        {
            private static final long serialVersionUID = 20070903L;

            public final Strata strata;

            public final String[] fields;

            public Index(Strata strata, String[] fields)
            {
                this.strata = strata;
                this.fields = fields;
            }
        }

        public final static class Creator
        {
            private final Set setOfBinNames;

            private final Map mapOfFields;

            private final List listOfAlternates;

            public Creator(Set setOfBinNames)
            {
                this.setOfBinNames = setOfBinNames;
                this.mapOfFields = new LinkedHashMap();
                this.listOfAlternates = new ArrayList();
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
                Set seen = new HashSet();
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
        implements Iterator
        {
            private final Advancer stored;

            private final Advancer isolated;

            private final Snapshot snapshot;

            private final Long[] keys;

            private final Schema schema;

            private Join.Record nextStored;

            private Join.Record nextIsolated;

            private Join.Record next;

            private final Map mapToScan;

            private final Index index;

            public Cursor(Snapshot snapshot, Long[] keys, Map mapToScan, Strata.Cursor storedCursor, Strata.Cursor isolatedCursor, Schema schema, Index index)
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
                        break;
                    }
                    Record record = cursor.getRecord();
                    if (keys.length > 0 && !partial(keys, record.keys))
                    {
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

            public Object next()
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
                    Strata.Query query = join.schema.indices[i].strata.query(BentoStorage.txn(join.mutator));
                    Strata.Cursor cursor = isolation[i].query(BentoStorage.txn(join.mutator)).first();
                    while (cursor.hasNext())
                    {
                        query.remove((Record) cursor.next());
                    }
                    cursor.release();
                    query.flush();
                }

            }

            public void dispose(Bento.Mutator mutator, boolean deallocate)
            {
                for (int i = 0; i < isolation.length; i++)
                {
                    isolation[i].query(BentoStorage.txn(mutator)).destroy();
                }
            }
        }
    }

    public final static class Tuple
    {
        private final Snapshot snapshot;

        private final String[] fields;

        private final Map mapOfFields;

        private final Join.Record record;

        public Tuple(Snapshot snapshot, Map mapOfFields, String[] fields, Join.Record record)
        {
            this.snapshot = snapshot;
            this.mapOfFields = mapOfFields;
            this.fields = fields;
            this.record = record;
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

        public Map getKeys()
        {
            Map mapOfKeys = new HashMap();
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
        public abstract Comparable[] getFields(Object object);
    }

    public final static class BeanExtractor
    implements FieldExtractor
    {
        private static final long serialVersionUID = 20070917L;

        private final Class type;

        private final String[] methods;

        public BeanExtractor(Class type, String[] fields)
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

        public Comparable[] getFields(Object object)
        {
            Comparable[] comparables = new Comparable[methods.length];
            for (int i = 0; i < methods.length; i++)
            {
                try
                {
                    comparables[i] = (Comparable) type.getMethod(methods[0], new Class[0]).invoke(object, new Object[0]);
                }
                catch (Exception e)
                {
                    throw new Danger("A.", e, 0);
                }
            }
            return comparables;
        }
    }

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
            Strata.Creator creator = new Strata.Creator();

            creator.setCacheFields(true);
            creator.setFieldExtractor(new Extractor());
            creator.setStorage(new ArrayListStorage());

            return creator.create(null);
        }

        public void add(Snapshot snapshot, Bento.Mutator mutator, Bin bin, Bag bag)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            Comparable[] fields = schema.extractor.getFields(bag.getObject());
            if (schema.notNull && hasNulls(fields))
            {
                throw new Error("Not null violation.", NOT_NULL_VIOLATION_ERROR);
            }
            if (schema.unique && (schema.notNull || !hasNulls(fields)))
            {
                Iterator found = find(snapshot, mutator, bin, fields, true);
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

        public void update(Snapshot snapshot, Bento.Mutator mutator, Bin bin, Bag bag, Long previous)
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
            Comparable[] fields = schema.extractor.getFields(bag.getObject());
            if (schema.notNull && hasNulls(fields))
            {
                throw new Error("Not null violation.", NOT_NULL_VIOLATION_ERROR);
            }
            if (schema.unique && (schema.notNull || !hasNulls(fields)))
            {
                Iterator exists = find(snapshot, mutator, bin, fields, true);
                if (exists.hasNext())
                {
                    Bag existing = (Bag) exists.next();
                    if (existing.getKey().equals(bag.getKey()))
                    {
                        throw new Error("unique.index.constraint.violation", UNIQUE_CONSTRAINT_VIOLATION_ERROR).put("bin", bin.getName());
                    }
                }
            }
            query.insert(new Record(bag.getKey(), bag.getVersion()));
            query.flush();
        }

        public void remove(Bento.Mutator mutator, Bin bin, Long key, Long version)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            final Bag bag = bin.get(schema.unmarshaller, key, version);
            Comparable[] fields = schema.extractor.getFields(bag.getObject());
            schema.strata.query(txn).remove(fields, new Strata.Deletable()
            {
                public boolean deletable(Object object)
                {
                    Record record = (Record) object;
                    return record.key.equals(bag.getKey()) && record.version.equals(bag.getVersion());
                }
            });
        }

        private Cursor find(Snapshot snapshot, Bento.Mutator mutator, Bin bin, Comparable[] fields, boolean limit)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            return new Cursor(schema.strata.query(txn).find(fields), isolation.query(txn).find(fields), txn, fields, limit);
        }

        private Cursor first(Snapshot snapshot, Bento.Mutator mutator, Bin bin)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            return new Cursor(schema.strata.query(txn).first(), isolation.query(txn).first(), txn, new Comparable[] {}, false);
        }

        private void commit(Snapshot snapshot, Bento.Mutator mutator, Bin bin)
        {
            Transaction txn = new Transaction(mutator, bin, schema);
            Strata.Query queryOfIsolated = isolation.query(txn);
            Strata.Query queryOfStored = schema.strata.query(txn);
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
                        Comparable[] fields = schema.extractor.getFields(bag.getObject());
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

            public void setExtractor(Class type, String field)
            {
                setExtractor(type, new String[] { field });
            }

            public void setExtractor(Class type, String[] fields)
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
        }

        public final static class Transaction
        implements BentoStorage.MutatorServer
        {
            public final Bento.Mutator mutator;

            public final Bin bin;

            public final Schema schema;

            public Transaction(Bento.Mutator mutator, Bin bin, Schema schema)
            {
                this.mutator = mutator;
                this.bin = bin;
                this.schema = schema;
            }

            public Bento.Mutator getMutator()
            {
                return mutator;
            }

            public Comparable[] getFields(Long key, Long version)
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
        implements Iterator
        {
            private final Comparable[] fields;

            private final Transaction txn;

            private final Strata.Cursor isolated;

            private final Strata.Cursor stored;

            private final boolean limit;

            private Record nextStored;

            private Record nextIsolated;

            private Bag next;

            public Cursor(Strata.Cursor stored, Strata.Cursor isolated, Transaction txn, Comparable[] fields, boolean limit)
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
        implements Iterator
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

    public final static class Snapshot
    {
        private final Strata snapshots;

        private final Map mapOfBinCommons;

        private final Map mapOfJoinSchemas;

        private final Map mapOfJanitors;

        private final Set setOfCommitted;

        private final Bento bento;

        private final Map mapOfBins;

        private final Map mapOfJoins;

        private final Long version;

        private final Test test;

        private final Long oldest;

        private boolean spent;

        public Snapshot(Strata snapshots, Map mapOfBinCommons, Map mapOfJoinSchemas, Bento bento, Set setOfCommitted, Test test, Long version)
        {
            this.snapshots = snapshots;
            this.mapOfBinCommons = mapOfBinCommons;
            this.mapOfJoinSchemas = mapOfJoinSchemas;
            this.bento = bento;
            this.mapOfBins = new HashMap();
            this.mapOfJoins = new HashMap();
            this.version = version;
            this.test = test;
            this.setOfCommitted = setOfCommitted;
            this.oldest = (Long) setOfCommitted.iterator().next();
            this.mapOfJanitors = new HashMap();
        }

        public Bin getBin(String name)
        {
            Bin bin = (Bin) mapOfBins.get(name);
            if (bin == null)
            {
                Bin.Common binCommon = (Bin.Common) mapOfBinCommons.get(name);
                bin = new Bin(this, bento.mutate(), name, binCommon, mapOfJanitors);
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
                join = new Join(this, bento.mutate(), schema, joinName, mapOfJanitors);
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

        public void commit()
        {
            if (spent)
            {
                throw new Danger("commit.spent.snapshot", 501);
            }

            spent = true;

            Iterator bins = mapOfBins.values().iterator();
            while (bins.hasNext())
            {
                Bin bin = (Bin) bins.next();
                bin.flush();
            }

            Iterator joins = mapOfJoins.values().iterator();
            while (joins.hasNext())
            {
                Join join = (Join) joins.next();
                join.flush();
            }

            try
            {
                bins = mapOfBins.values().iterator();
                while (bins.hasNext())
                {
                    Bin bin = (Bin) bins.next();
                    bin.commit();
                }

                joins = mapOfJoins.values().iterator();
                while (joins.hasNext())
                {
                    Join join = (Join) joins.next();
                    join.commit();
                }
            }
            catch (Error e)
            {
                test.changesWritten();

                Bento.Mutator mutator = bento.mutate();

                Iterator janitors = mapOfJanitors.entrySet().iterator();
                while (janitors.hasNext())
                {
                    Map.Entry entry = (Map.Entry) janitors.next();
                    Bento.Address address = (Bento.Address) entry.getKey();
                    Janitor janitor = (Janitor) entry.getValue();

                    janitor.rollback(this);

                    mutator.free(mutator.load(address));
                    janitor.dispose(mutator, true);
                    mutator.getJournal().commit();
                }

                Strata.Query query = snapshots.query(BentoStorage.txn(mutator));
                query.remove(new Comparable[] { version }, Strata.ANY);

                test.journalComplete.release();

                throw e;
            }

            test.changesWritten();

            Bento.Mutator mutator = bento.mutate();

            Iterator janitors = mapOfJanitors.entrySet().iterator();
            while (janitors.hasNext())
            {
                Map.Entry entry = (Map.Entry) janitors.next();
                Bento.Address address = (Bento.Address) entry.getKey();
                Janitor janitor = (Janitor) entry.getValue();
                mutator.free(mutator.load(address));
                janitor.dispose(mutator, false);
            }

            Strata.Query query = snapshots.query(BentoStorage.txn(mutator));

            Record committed = new Record(version, COMMITTED);
            query.insert(committed);

            query.remove(new Comparable[] { version }, Strata.ANY);

            test.journalComplete.release();
        }

        public void rollback()
        {
            // FIXME Rethink. Cannot reuse.
            if (!spent)
            {
                spent = true;
                Bento.Mutator mutator = bento.mutate();

                Iterator janitors = mapOfJanitors.entrySet().iterator();
                while (janitors.hasNext())
                {
                    Map.Entry entry = (Map.Entry) janitors.next();
                    Bento.Address address = (Bento.Address) entry.getKey();
                    Janitor janitor = (Janitor) entry.getValue();
                    mutator.free(mutator.load(address));
                    janitor.dispose(mutator, true);
                }

                Strata.Query query = snapshots.query(BentoStorage.txn(mutator));

                query.remove(new Comparable[] { version }, Strata.ANY);
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

    public final static Test newTest()
    {
        return new Test(new Latch(), new Latch(), new Latch());
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
            try
            {
                registerMutation.acquire();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void waitForChangesWritten()
        {
            try
            {
                changesWritten.acquire();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void registerMutation()
        {
            registerMutation.release();
        }

        public void waitForCompletion()
        {
            try
            {
                journalComplete.acquire();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */