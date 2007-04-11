/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.ref.ReferenceQueue;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import EDU.oswego.cs.dl.util.concurrent.Mutex;

import com.agtrz.bento.Bento;
import com.agtrz.dynamic.MetaClass;
import com.agtrz.dynamic.Property;
import com.agtrz.strata.Strata;
import com.agtrz.strata.bento.BentoStorage;
import com.agtrz.swag.danger.AsinineCheckedExceptionThatIsEntirelyImpossible;
import com.agtrz.swag.danger.Danger;
import com.agtrz.swag.io.ByteBufferInputStream;
import com.agtrz.swag.io.ByteReader;
import com.agtrz.swag.io.ByteWriter;
import com.agtrz.swag.io.SizeOf;
import com.agtrz.swag.util.Queueable;
import com.agtrz.swag.util.WeakMapValue;

public class Depot
{
    private final static URI MONKEY_URI = URI.create("http://syndibase.agtrz.com/strata");

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
            bin.getJoin(name).add(this, bags);
        }
        
        public Iterator getLinked(String name)
        {
            Join join = bin.getJoin(name);
            return join.find(new Bag[] { this });
        }
    }

    private final static Integer OPERATING = new Integer(0);

    private final static Integer ROLLEDBACK = new Integer(0);

    private final static Integer COMMITTED = new Integer(0);

    private final static class MutationRecord
    {
        public final Long version;

        public final Integer state;

        public MutationRecord(Long version, Integer state)
        {
            this.version = version;
            this.state = state;
        }

        public boolean equals(Object object)
        {
            if (object instanceof MutationRecord)
            {
                MutationRecord record = (MutationRecord) object;
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

    private final static class MutationExtractor
    implements Strata.FieldExtractor, Serializable
    {
        private static final long serialVersionUID = 20070409L;

        public Comparable[] getFields(Object object)
        {
            MutationRecord record = (MutationRecord) object;
            return new Comparable[] { record.version };
        }
    }

    private final static class MutationWriter
    implements ByteWriter, Serializable
    {
        private static final long serialVersionUID = 20070409L;

        public int getSize(Object object)
        {
            return SizeOf.LONG + SizeOf.INTEGER;
        }

        public void write(ByteBuffer bytes, Object object)
        {
            if (object == null)
            {
                bytes.putLong(0L);
                bytes.putInt(0);
            }
            else
            {
                MutationRecord record = (MutationRecord) object;
                bytes.putLong(record.version.longValue());
                bytes.putInt(record.state.intValue());
            }
        }
    }

    private final static class MutationReader
    implements ByteReader, Serializable
    {
        private static final long serialVersionUID = 20070409L;

        public Object read(ByteBuffer bytes)
        {
            MutationRecord record = new MutationRecord(new Long(bytes.getLong()), new Integer(bytes.getInt()));
            return record.version.longValue() == 0L ? null : record;
        }
    }

    private final static class Revision
    {
        private final Long version;

        private final Bento.Address address;

        public Revision(Long version, Bento.Address address)
        {
            this.version = version;
            this.address = address;
        }

        public Long getVersion()
        {
            return version;
        }

        public final Bento.Address getAddress()
        {
            return address;
        }
    }

    private final static class History
    {
        private final List listOfRevisions = new ArrayList();

        private final Mutex mutex = new Mutex();

        private final Long objectKey;

        public History(Long objectKey)
        {
            this.objectKey = objectKey;
        }

        public Long getObjectKey()
        {
            return objectKey;
        }

        public Mutex getMutex()
        {
            return mutex;
        }

        public synchronized Revision getLastRevision()
        {
            return (Revision) listOfRevisions.get(listOfRevisions.size() - 1);
        }

        public void add(Revision revision)
        {
            listOfRevisions.add(revision);
        }

        public boolean isEmpty()
        {
            return listOfRevisions.size() == 0;
        }

        public String toString()
        {
            return listOfRevisions.toString();
        }
    }

    private final static class BinRecordExtractor
    implements Strata.FieldExtractor, Serializable
    {
        private static final long serialVersionUID = 20070408L;

        public Comparable[] getFields(Object object)
        {
            BinRecord record = (BinRecord) object;
            return new Comparable[] { record.key, record.version };
        }
    }

    private final static class BinRecord
    {
        public final Long key;

        public final Long version;

        public final Bento.Address address;

        public BinRecord(Long key, Long version, Bento.Address address)
        {
            this.key = key;
            this.version = version;
            this.address = address;
        }

        public boolean equals(Object object)
        {
            if (object instanceof BinRecord)
            {
                BinRecord record = (BinRecord) object;
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

    private final static class BinRecordWriter
    implements ByteWriter, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public int getSize(Object object)
        {
            return SizeOf.LONG * 2 + Bento.ADDRESS_SIZE;
        }

        public void write(ByteBuffer out, Object object)
        {
            if (object == null)
            {
                out.putLong(0L);
                out.putLong(0L);
                out.putLong(0L);
                out.putInt(0);
            }
            else
            {
                BinRecord record = (BinRecord) object;
                out.putLong(record.key.longValue());
                out.putLong(record.version.longValue());
                out.putLong(record.address.getPosition());
                out.putInt(record.address.getBlockSize());
            }
        }
    }

    private final static class BinRecordReader
    implements ByteReader, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public Object read(ByteBuffer in)
        {
            BinRecord record = new BinRecord(new Long(in.getLong()), new Long(in.getLong()), new Bento.Address(in.getLong(), in.getInt()));
            return record.key.longValue() == 0L ? null : record;
        }
    }

    public final static class BinCreator
    {
        private final String name;

        private final Map mapOfIndices;

        private final Map mapOfJoinCreators;

        public BinCreator(String name)
        {
            this.name = name;
            this.mapOfIndices = new HashMap();
            this.mapOfJoinCreators = new HashMap();
        }

        public String getName()
        {
            return name;
        }

        public IndexCreator newIndex(String name)
        {
            IndexCreator newIndex = new IndexCreator();
            mapOfIndices.put(name, newIndex);
            return newIndex;
        }

        public JoinCreator newJoin(String name)
        {
            if (mapOfJoinCreators.containsKey(name))
            {
                throw new IllegalStateException();
            }
            JoinCreator newJoin = new JoinCreator(getName(), getName());
            mapOfJoinCreators.put(name, newJoin);
            return newJoin;
        }

        public void addIndex(String name, Strata.FieldExtractor fields)
        {
            IndexCreator newIndex = newIndex(name);
            newIndex.setFields(fields);
        }

    }

    private final static class HistoryCache
    {
        protected final transient ReferenceQueue queue = new ReferenceQueue();

        private final Map mapOfHistories = new HashMap();

        private void collect()
        {
            WeakMapValue reference = null;
            while ((reference = (WeakMapValue) queue.poll()) != null)
            {
                ((Queueable) reference).dequeue();
            }
        }

        public History get(Long key)
        {
            collect();

            History history = null;
            WeakMapValue reference = (WeakMapValue) mapOfHistories.get(key);
            if (reference != null)
            {
                history = (History) reference.get();
            }

            if (history == null)
            {
                history = new History(key);
                mapOfHistories.put(key, new WeakMapValue(key, history, mapOfHistories, queue));
            }

            return history;
        }
    }

    private final static class BinSchema
    implements Serializable
    {
        private static final long serialVersionUID = 20070408L;

        public final Strata strata;

        public final Map mapOfJoins;

        public BinSchema(Strata strata, Map mapOfJoins)
        {
            this.strata = strata;
            this.mapOfJoins = mapOfJoins;
        }
    }

    private final static class JoinSchema
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public final Map mapOfFields;

        public final Strata strata;

        public JoinSchema(Strata strata, Map mapOfFields)
        {
            this.mapOfFields = mapOfFields;
            this.strata = strata;
        }
    }

    private final static class JoinCommon
    {
        public final Map mapOfFields;

        public final Strata strata;

        public final HistoryCache histories;

        public JoinCommon(Strata strata, Map mapOfFields)
        {
            this.strata = strata;
            this.mapOfFields = mapOfFields;
            this.histories = new HistoryCache();
        }
    }

    public final static class Creator
    {
        private final Map mapOfBinCreators = new HashMap();

        public BinCreator newBin(String name)
        {
            BinCreator newBin = new BinCreator(name);
            mapOfBinCreators.put(name, newBin);
            return newBin;
        }

        public Depot create(File file)
        {
            Bento.Creator newBento = new Bento.Creator();
            newBento.addStaticPage(MONKEY_URI, Bento.ADDRESS_SIZE);
            Bento bento = newBento.create(file);
            Bento.Mutator mutator = bento.mutate(bento.newNullJournal());

            BentoStorage.Creator newMutationStorage = new BentoStorage.Creator();

            newMutationStorage.setWriter(new MutationWriter());
            newMutationStorage.setReader(new MutationReader());

            Strata.Creator newMutationStrata = new Strata.Creator();

            newMutationStrata.setStorage(newMutationStorage.create());
            newMutationStrata.setFieldExtractor(new MutationExtractor());

            Strata mutations = newMutationStrata.create(BentoStorage.txn(mutator));

            Map mapOfBins = new HashMap();
            Iterator bags = mapOfBinCreators.entrySet().iterator();
            while (bags.hasNext())
            {
                Map.Entry entry = (Map.Entry) bags.next();
                String name = (String) entry.getKey();

                BentoStorage.Creator newBinStorage = new BentoStorage.Creator();
                newBinStorage.setWriter(new BinRecordWriter());
                newBinStorage.setReader(new BinRecordReader());

                Strata.Creator newBinStrata = new Strata.Creator();

                newBinStrata.setStorage(newBinStorage.create());
                newBinStrata.setFieldExtractor(new BinRecordExtractor());

                Strata strata = newBinStrata.create(BentoStorage.txn(mutator));

                BinCreator newBin = (BinCreator) entry.getValue();

                Map mapOfJoins = new HashMap();
                Iterator joins = newBin.mapOfJoinCreators.entrySet().iterator();
                while (joins.hasNext())
                {
                    Map.Entry join = (Map.Entry) joins.next();
                    String joinName = (String) join.getKey();
                    JoinCreator newJoin = (JoinCreator) join.getValue();
                    Map mapOfFields = new LinkedHashMap(newJoin.mapOfFields);

                    BentoStorage.Creator newJoinStorage = new BentoStorage.Creator();
                    newJoinStorage.setWriter(new JoinWriter(mapOfFields.size()));
                    newJoinStorage.setReader(new JoinReader(mapOfFields.size()));

                    Strata.Creator newJoinStrata = new Strata.Creator();

                    newJoinStrata.setStorage(newJoinStorage.create());
                    newJoinStrata.setFieldExtractor(new JoinExtractor());

                    Strata joinStrata = newJoinStrata.create(BentoStorage.txn(mutator));

                    mapOfJoins.put(joinName, new JoinSchema(joinStrata, mapOfFields));
                }
                mapOfBins.put(name, new BinSchema(strata, mapOfJoins));
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

            Bento.Block block = mutator.load(bento.getStaticAddress(MONKEY_URI));

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
            Bento bento = new Bento.Opener().open(file);
            Bento.Mutator mutator = bento.mutate();
            Bento.Block block = mutator.load(bento.getStaticAddress(MONKEY_URI));
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
                BinSchema binSchema = (BinSchema) bin.getValue();
                Map mapOfJoinCommons = new HashMap();
                Iterator joins = binSchema.mapOfJoins.entrySet().iterator();
                while (joins.hasNext())
                {
                    Map.Entry join = (Map.Entry) joins.next();
                    JoinSchema joinSchema = (JoinSchema) join.getValue();
                    JoinCommon joinCommon = new JoinCommon(joinSchema.strata, new LinkedHashMap(joinSchema.mapOfFields));
                    mapOfJoinCommons.put(join.getKey(), joinCommon);
                }
                BinCommon binCommon = new BinCommon(binSchema.strata, mapOfJoinCommons);
                mapOfBinCommons.put(bin.getKey(), binCommon);
            }

            return new Depot(file, bento, mutations, mapOfBinCommons);
        }
    }

    public final static class JoinCreator
    {
        private final HashMap mapOfFields = new LinkedHashMap();

        public JoinCreator(String fieldName, String binName)
        {
            mapOfFields.put(fieldName, binName);
        }

        public JoinCreator add(String fieldName, BinCreator newBin)
        {
            mapOfFields.put(fieldName, newBin.getName());
            return this;
        }

        public JoinCreator add(BinCreator newBin)
        {
            mapOfFields.put(newBin.getName(), newBin.getName());
            return this;
        }
    }

    public final static class IndexCreator
    {
        private Strata.FieldExtractor fields;

        public void setFields(Strata.FieldExtractor fields)
        {
            this.fields = fields;
        }

        public Strata.FieldExtractor getFields()
        {
            return fields;
        }
    }

    public interface Serializer
    {
        public Object add(Object object);

        public void update(Object key, Object object);

        public void delete(Object key);
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

    public final static class BinCommon
    {
        public final Strata strata;

        public final Map mapOfJoinCommons;

        public final HistoryCache histories;

        public BinCommon(Strata strata, Map mapOfJoinCommons)
        {
            this.strata = strata;
            this.mapOfJoinCommons = mapOfJoinCommons;
            this.histories = new HistoryCache();
        }
    }

    public final class Bin
    {
        private final String name;

        private final Snapshot snapshot;

        private final BinCommon common;

        private final Strata.Query query;

        private final Map mapOfJoins = new HashMap();

        public final Map mapOfObjects = new LinkedHashMap();

        public Bin(Snapshot snapshot, String name, BinCommon common)
        {
            this.snapshot = snapshot;
            this.name = name;
            this.common = common;
            this.query = common.strata.query(snapshot);
        }

        public String getName()
        {
            return name;
        }

        public Bag add(Marshaller marshaller, Object object)
        {
            Bag bag = new Bag(this, new Long(++identifier), snapshot.getVersion(), object);
            Bento.OutputStream allocation = new Bento.OutputStream(snapshot.mutator);
            marshaller.marshall(allocation, object);
            Bento.Address address = allocation.allocate(false);
            query.insert(new BinRecord(bag.getKey(), bag.getVersion(), address));
            return bag;
        }

        public Bag get(Unmarshaller unmarshaller, Long key)
        {
            Strata.Cursor cursor = query.find(new Comparable[] { key });
            if (!cursor.hasNext())
            {
                return null;
            }
            BinRecord record = (BinRecord) cursor.next();
            if (!record.key.equals(key))
            {
                return null;
            }
            Object object = snapshot.mapOfObjects.get(record.key);
            if (object == null)
            {
                Bento.Block block = snapshot.getMutator().load(record.address);
                object = unmarshaller.unmarshall(new ByteBufferInputStream(block.toByteBuffer(), false));
                snapshot.mapOfObjects.put(record.key, object);
            }
            return new Bag(this, key, record.version, object);
        }

        public Join getJoin(String name)
        {
            Join join = (Join) mapOfJoins.get(name);
            if (join == null)
            {
                JoinCommon joinCommon = (JoinCommon) common.mapOfJoinCommons.get(name);
                join = new Join(snapshot, joinCommon);
            }
            return join;
        }
    }

    private final static class JoinRecord
    {
        public final Long[] objectKeys;

        public final Long version;

        public final boolean deleted;

        public JoinRecord(Long[] objectKeys, Long version, boolean deleted)
        {
            this.objectKeys = objectKeys;
            this.version = version;
            this.deleted = deleted;
        }

        public boolean equals(Object object)
        {
            if (object instanceof JoinRecord)
            {
                JoinRecord record = (JoinRecord) object;
                Long[] left = objectKeys;
                Long[] right = record.objectKeys;
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
            for (int i = 0; i < objectKeys.length; i++)
            {
                hashCode = hashCode * 37 + objectKeys[i].hashCode();
            }
            hashCode = hashCode * 37 + version.hashCode();
            hashCode = hashCode * 37 + (deleted ? 1 : 0);
            return hashCode;
        }
    }

    private final static class JoinExtractor
    implements Strata.FieldExtractor, Serializable
    {
        private static final long serialVersionUID = 20070403L;

        public Comparable[] getFields(Object object)
        {
            JoinRecord record = (JoinRecord) object;
            Comparable[] fields = new Comparable[record.objectKeys.length + 2];
            Long[] keys = record.objectKeys;
            for (int i = 0; i < keys.length; i++)
            {
                fields[i] = keys[i];
            }
            fields[keys.length] = record.version;
            fields[keys.length + 1] = record.deleted ? Boolean.TRUE : Boolean.FALSE;
            return fields;
        }
    }

    private final static class JoinWriter
    implements ByteWriter, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final int size;

        public JoinWriter(int size)
        {
            this.size = size;
        }

        public int getSize(Object object)
        {
            return SizeOf.LONG * size + SizeOf.LONG + SizeOf.SHORT;
        }

        public void write(ByteBuffer bytes, Object object)
        {
            if (object == null)
            {
                for (int i = 0; i < size; i++)
                {
                    bytes.putLong(0L);
                }
                bytes.putLong(0L);
                bytes.putShort((short) 0);
            }
            else
            {
                JoinRecord record = (JoinRecord) object;
                Long[] keys = record.objectKeys;
                for (int i = 0; i < size; i++)
                {
                    bytes.putLong(keys[i].longValue());
                }
                bytes.putLong(record.version.longValue());
                bytes.putShort(record.deleted ? (short) 1 : (short) 0);
            }
        }
    }

    private final static class JoinReader
    implements ByteReader, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final int size;

        public JoinReader(int size)
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
            JoinRecord record = new JoinRecord(keys, new Long(bytes.getLong()), bytes.getShort() == 1);
            return record.version.longValue() == 0L ? null : record;
        }
    }

    public final static class Join
    {
        private final Snapshot snapshot;

        private final JoinCommon joinCommon;

        private final Strata.Query query;

        public Join(Snapshot snapshot, JoinCommon joinCommon)
        {
            this.snapshot = snapshot;
            this.query = joinCommon.strata.query(snapshot);
            this.joinCommon = joinCommon;
        }
        
        public void add(Bag bag, Bag[] bags)
        {
            Long[] keys = new Long[bags.length + 1];

            keys[0] = bag.getKey();
            
            for (int i = 0; i < bags.length; i++)
            {
                keys[i + 1] = bags[i].getKey();
            }

            add(keys, snapshot.getVersion(), false);

        }

        // FIXME rename link
        public void add(Bag[] bags)
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
            // FIXME Only unique.
            JoinRecord record = new JoinRecord(keys, version, deleted);
            query.insert(record);
        }

        public Iterator find(Bag[] bags)
        {
            Long[] keys = new Long[bags.length];

            for (int i = 0; i < bags.length; i++)
            {
                keys[i] = bags[i].getKey();
            }

            return find(keys);
        }

        public Iterator find(Long[] keys)
        {
            return new JoinIterator(snapshot, query.find(keys), joinCommon);
        }
    }

    private final static class JoinIterator
    implements Iterator
    {
        private final Strata.Cursor cursor;

        private final Depot.Snapshot mutator;

        private final Map.Entry[] fieldMappings;

        public JoinIterator(Depot.Snapshot mutator, Strata.Cursor cursor, JoinCommon joinCommon)
        {
            this.mutator = mutator;
            this.cursor = cursor;
            this.fieldMappings = (Map.Entry[]) joinCommon.mapOfFields.entrySet().toArray(new Map.Entry[joinCommon.mapOfFields.size()]);
        }

        public boolean hasNext()
        {
            return cursor.hasNext();
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public Object next()
        {
            Unmarshaller unmarshaller = new SerializationUnmarshaller();
            JoinRecord record = (JoinRecord) cursor.next();
            Bag[] bags = new Bag[record.objectKeys.length];
            for (int i = 0; i < bags.length; i++)
            {
                String bagName = (String) fieldMappings[i].getValue();
                bags[i] = mutator.getBin(bagName).get(unmarshaller, record.objectKeys[i]);
            }
            return bags;
        }
    }

    public final static class Index
    {
        private final MetaClass metaClass;

        private final List listOfFields;

        public Index(MetaClass metaClass)
        {
            this.metaClass = metaClass;
            this.listOfFields = new ArrayList();
        }

        public void add(String fieldName)
        {
            Property property = metaClass.getFunctionDictionary().getProperty(fieldName);
            listOfFields.add(property);
        }
    }

    private final Map mapOfBinCommons;

    private final Bento bento;

    private final Strata mutations;

    private long identifier;

    public Depot(File file, Bento bento, Strata mutations, Map mapOfBinCommons)
    {
        this.identifier = 0;
        this.mapOfBinCommons = mapOfBinCommons;
        this.mutations = mutations;
        this.bento = bento;
    }

    public void close()
    {
        bento.close();
    }

    public Snapshot newSnapshot()
    {
        Long version = new Long(System.currentTimeMillis());
        MutationRecord record = new MutationRecord(version, OPERATING);
        Bento.Mutator mutator = bento.mutate();

        Strata.Query query = mutations.query(BentoStorage.txn(mutator));
        query.insert(record);
        query.write();
        
        return new Snapshot(mutator, query);
    }

    public class Snapshot
    implements BentoStorage.MutatorServer
    {
        private final Bento.Mutator mutator;

        private final Map mapOfBags;

        private final Map mapOfObjects = new HashMap();

        private final Map mapOfJoins;

        private final Long version;
        
        private final Strata.Query query;

        public Snapshot(Bento.Mutator mutator, Strata.Query query)
        {
            this.mutator = mutator;
            this.mapOfBags = new HashMap();
            this.mapOfJoins = new HashMap();
            this.version = new Long(System.currentTimeMillis());
            this.query = query;
        }

        public Bin getBin(String name)
        {
            Bin bin = (Bin) mapOfBags.get(name);
            if (bin == null)
            {
                BinCommon binCommon = (BinCommon) mapOfBinCommons.get(name);
                bin = new Bin(this, name, binCommon);
                mapOfBags.put(name, bin);
            }
            return bin;
        }

        public Long getVersion()
        {
            return version;
        }

        public void commit()
        {
            if (mapOfBags.size() != 0 || mapOfJoins.size() != 0)
            {
                Iterator bags = mapOfBags.values().iterator();
                while (bags.hasNext())
                {
                    Bin bag = (Bin) bags.next();
                    bag.query.write();
                }
                mapOfBags.clear();
                Iterator joins = mapOfJoins.values().iterator();
                while (joins.hasNext())
                {
                    Join join = (Join) joins.next();
                    join.query.write();
                }
                mapOfJoins.clear();
                mutator.getJournal().commit();

                MutationRecord committed = new MutationRecord(version, COMMITTED);
                query.insert(committed);
                query.write();
            }
        }

        public void rollback()
        {
            if (mapOfBags.size() != 0 || mapOfJoins.size() != 0)
            {
                Iterator bags = mapOfBags.values().iterator();
                while (bags.hasNext())
                {
                    Bin bag = (Bin) bags.next();
                    bag.query.revert();
                }
                mapOfBags.clear();
                Iterator joins = mapOfJoins.values().iterator();
                while (joins.hasNext())
                {
                    Join join = (Join) joins.next();
                    join.query.revert();
                }
                mapOfJoins.clear();
                mutator.getJournal().rollback();
                
                // FIXME Maybe just delete operating?
                MutationRecord rolledback = new MutationRecord(version, ROLLEDBACK);
                query.insert(rolledback);
                query.write();
}
        }

        public Bento.Mutator getMutator()
        {
            return mutator;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */