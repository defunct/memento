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

import EDU.oswego.cs.dl.util.concurrent.NullSync;
import EDU.oswego.cs.dl.util.concurrent.Sync;

import com.agtrz.bento.Bento;
import com.agtrz.dynamic.MetaClass;
import com.agtrz.dynamic.Property;
import com.agtrz.strata.ArrayListStorage;
import com.agtrz.strata.Strata;
import com.agtrz.strata.bento.BentoStorage;
import com.agtrz.swag.danger.AsinineCheckedExceptionThatIsEntirelyImpossible;
import com.agtrz.swag.danger.Danger;
import com.agtrz.swag.io.ByteBufferInputStream;
import com.agtrz.swag.io.ByteReader;
import com.agtrz.swag.io.ByteWriter;
import com.agtrz.swag.io.SizeOf;

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

    private final static Integer COMMITTED = new Integer(1);

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
            Long version = new Long(bytes.getLong());
            Integer state = new Integer(bytes.getInt());
            if (version.longValue() == 0L)
            {
                return null;
            }
            MutationRecord record = new MutationRecord(version, state);
            return record;
        }
    }

    // FIXME Rename BinExtractor.
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

        public JoinCommon(Strata strata, Map mapOfFields)
        {
            this.strata = strata;
            this.mapOfFields = mapOfFields;
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

            Object txn = BentoStorage.txn(mutator);
            Strata mutations = newMutationStrata.create(txn);

            Strata.Query query = mutations.query(txn);
            query.insert(new MutationRecord(new Long(1L), COMMITTED));
            query.write();

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

                long identifer = 1L;
                Strata.Query query = binSchema.strata.query(BentoStorage.txn(mutator));
                Strata.Cursor last = query.last();
                while (last.hasPrevious())
                {
                    BinRecord record = (BinRecord) last.previous();
                    identifer = record.key.longValue() + 1;
                }
                BinCommon binCommon = new BinCommon(binSchema.strata, mapOfJoinCommons, identifer);
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

        private long identifier;

        public BinCommon(Strata strata, Map mapOfJoinCommons, long identifier)
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

    public final static class Bin
    {
        private final String name;

        private final Snapshot snapshot;

        private final BinCommon common;

        private final Strata.Query query;

        private final Map mapOfJoins = new HashMap();

        private final Strata.Query isolation;

        public final Map mapOfObjects = new LinkedHashMap();

        public Bin(Snapshot snapshot, String name, BinCommon common)
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
            creator.setFieldExtractor(new BinRecordExtractor());
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
            isolation.insert(new BinRecord(bag.getKey(), bag.getVersion(), address));
            return bag;
        }

        public Bag update(Marshaller marshaller, Long key, Object object)
        {
            BinRecord record = get(key);
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
            isolation.insert(new BinRecord(bag.getKey(), bag.getVersion(), address));
            return bag;
        }

        public void delete(Long key)
        {
            BinRecord record = get(key);
            if (record == null)
            {
                Danger danger = new Danger();

                danger.source(Depot.class);
                danger.message("delete.bag.does.not.exist");

                throw danger;
            }
            isolation.insert(new BinRecord(key, snapshot.getVersion(), Bento.NULL_ADDRESS));
        }

        private BinRecord get(Strata.Cursor cursor, Long key, boolean isolated)
        {
            BinRecord candidate = null;
            for (;;)
            {
                if (!cursor.hasNext())
                {
                    break;
                }
                BinRecord record = (BinRecord) cursor.next();
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

        private static boolean isNull(BinRecord record)
        {
            return record.address.equals(Bento.NULL_ADDRESS);
        }

        private Bag unmarshall(Unmarshaller unmarshaller, BinRecord record)
        {
            Bento.Block block = snapshot.getMutator().load(record.address);
            Object object = unmarshaller.unmarshall(new ByteBufferInputStream(block.toByteBuffer(), false));
            return new Bag(this, record.key, record.version, object);
        }

        private BinRecord get(Long key)
        {
            BinRecord stored = get(query.find(new Comparable[] { key }), key, false);
            BinRecord isolated = get(isolation.find(new Comparable[] { key }), key, true);
            if (isolated != null)
            {
                return isNull(isolated) ? null : isolated;
            }
            else if (stored != null)
            {
                return isNull(stored) ? null : stored;
            }
            return null;
        }

        public Bag get(Unmarshaller unmarshaller, Long key)
        {
            BinRecord record = get(key);
            return record == null ? null : unmarshall(unmarshaller, record);
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

        private void commit()
        {
            Strata.Cursor isolated = isolation.first();
            if (isolated.hasNext())
            {
                BinRecord first = (BinRecord) isolated.next();
                while (first != null)
                {
                    BinRecord next = null;
                    BinRecord record = first;
                    for (;;)
                    {
                        if (!isolated.hasNext())
                        {
                            break;
                        }
                        next = (BinRecord) isolated.next();
                        if (!next.key.equals(first.key))
                        {
                            break;
                        }
                        record = next;
                    }
                    query.insert(record);
                    first = next;
                }
                query.write();
            }
        }

        private void rollback()
        {
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

        private final Strata.Query isloation;

        public Join(Snapshot snapshot, JoinCommon joinCommon)
        {
            this.snapshot = snapshot;
            this.query = joinCommon.strata.query(snapshot);
            this.isloation = newIsolation();
            this.joinCommon = joinCommon;
        }
        
        
        private static Strata.Query newIsolation()
        {
            Strata.Creator creator = new Strata.Creator();

            creator.setCacheFields(true);
            creator.setFieldExtractor(new JoinExtractor());
            creator.setStorage(new ArrayListStorage());

            return creator.create(null).query(null);
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
        MutationRecord record = new MutationRecord(version, OPERATING);
        Bento.Mutator mutator = bento.mutate();

        Strata.Query query = mutations.query(BentoStorage.txn(mutator));

        Set setOfCommitted = new TreeSet();
        Strata.Cursor versions = query.first();
        while (versions.hasNext())
        {
            MutationRecord mutation = (MutationRecord) versions.next();
            if (mutation.state.equals(COMMITTED))
            {
                setOfCommitted.add(mutation.version);
            }
        }

        query.insert(record);
        query.write();

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

        private final Map mapOfJoins;

        private final Long version;

        private final Test test;

        private final Long oldest;

        private final Set setOfCommitted;

        public Snapshot(Bento.Mutator mutator, Strata.Query query, Set setOfCommitted, Test test, Long version)
        {
            this.mutator = mutator;
            this.mapOfBins = new HashMap();
            this.mapOfJoins = new HashMap();
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
                BinCommon binCommon = (BinCommon) mapOfBinCommons.get(name);
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

                MutationRecord committed = new MutationRecord(version, COMMITTED);
                query.insert(committed);
                query.write();

                mutator.getJournal().commit();

                test.journalComplete.release();
            }
        }

        public void rollback()
        {
            if (mapOfBins.size() != 0 || mapOfJoins.size() != 0)
            {
                Iterator bags = mapOfBins.values().iterator();
                while (bags.hasNext())
                {
                    Bin bin = (Bin) bags.next();
                    bin.rollback();
                }
                mapOfBins.clear();

                Strata.Query query = mutations.query(BentoStorage.txn(mutator));

                MutationRecord rolledback = new MutationRecord(version, COMMITTED);
                query.remove(rolledback);
                query.write();

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