/* Copyright Alan Gutierrez 2006 */
package com.agtrz.syndibase;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.agtrz.bento.Bento;
import com.agtrz.dynamic.MetaClass;
import com.agtrz.dynamic.Property;
import com.agtrz.strata.Strata;
import com.agtrz.strata.Strata.Cursor;
import com.agtrz.strata.bento.BentoStorage;
import com.agtrz.strata.bento.BentoStorage.MutatorServer;
import com.agtrz.swag.danger.AsinineCheckedExceptionThatIsEntirelyImpossible;
import com.agtrz.swag.danger.Danger;
import com.agtrz.swag.io.ByteBufferInputStream;
import com.agtrz.swag.io.ByteReader;
import com.agtrz.swag.io.ByteWriter;
import com.agtrz.swag.io.SizeOf;

public class Storage
{
    private final static URI MONKEY_URI = URI.create("http://syndibase.agtrz.com/strata");

    public static class RecordReference
    implements Comparable, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Integer bagKey;

        private final Long key;

        public RecordReference(Integer bagKey, Long key)
        {
            this.bagKey = bagKey;
            this.key = key;
        }

        // public RecordReference(int bagKey, long key)
        // {
        // this(new Integer(bagKey), new Long(key));
        // }

        public Integer getBagKey()
        {
            return bagKey;
        }

        public Long getKey()
        {
            return key;
        }

        public int compareTo(Object object)
        {
            RecordReference reference = (RecordReference) object;
            int compare = bagKey.compareTo(reference.bagKey);
            if (compare == 0)
            {
                return key.compareTo(reference.key);
            }
            return compare;
        }

        public int hashCode()
        {
            int hashCode = 1;
            hashCode = hashCode * 37 + bagKey.hashCode();
            hashCode = hashCode * 37 + key.hashCode();
            return hashCode;
        }

        public boolean equals(Object object)
        {
            if (object instanceof RecordReference)
            {
                RecordReference reference = (RecordReference) object;
                return bagKey.equals(reference.bagKey) && key.equals(reference.key);
            }
            return false;
        }
    }

    public static class Record
    implements Serializable
    {
        private static final long serialVersionUID = 20070210L;

        private final RecordReference reference;

        private final Object object;

        public Record(RecordReference reference, Object object)
        {
            this.reference = reference;
            this.object = object;
        }

        public RecordReference getReference()
        {
            return reference;
        }

        public Object getObject()
        {
            return object;
        }
    }

    private final static class IndexRecord
    {
        public final Long key;

        public final Bento.Address address;

        public IndexRecord(Long key, Bento.Address address)
        {
            this.key = key;
            this.address = address;
        }
    }

    private final static class IndexRecordWriter
    implements ByteWriter, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public int getSize(Object object)
        {
            return SizeOf.LONG + Bento.ADDRESS_SIZE;
        }

        public void write(ByteBuffer out, Object object)
        {
            if (object == null)
            {
                out.putLong(0L);
                out.putLong(0L);
                out.putInt(0);
            }
            else
            {
                IndexRecord record = (IndexRecord) object;
                out.putLong(record.key.longValue());
                out.putLong(record.address.getPosition());
                out.putInt(record.address.getBlockSize());
            }
        }
    }

    private final static class IndexRecordReader
    implements ByteReader, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public Object read(ByteBuffer in)
        {
            IndexRecord record = new IndexRecord(new Long(in.getLong()), new Bento.Address(in.getLong(), in.getInt()));
            return record.key.longValue() == 0L ? null : record;
        }
    }

    private final static class IndexRecordObjectResolver
    implements Strata.Resolver, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public Object resolve(Object txn, Object object)
        {
            Storage.Mutator mutator = (Storage.Mutator) txn;
            IndexRecord indexRecord = (IndexRecord) object;
            Record record = (Record) mutator.mapOfObjects.get(indexRecord.key);
            if (record == null)
            {
                Bento.Block block = mutator.getMutator().load(indexRecord.address);
                try
                {
                    ObjectInputStream in = new ObjectInputStream(new ByteBufferInputStream(block.toByteBuffer(), false));
                    record = (Record) in.readObject();
                }
                catch (IOException e)
                {
                    throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
                }
                catch (ClassNotFoundException e)
                {
                    Danger danger = new StorageException(e);

                    danger.source(Storage.class);
                    danger.message("class.not.found");

                    throw danger;
                }
                mutator.mapOfObjects.put(indexRecord.key, record);
            }
            return record;
        }
    }

    private final static class IndexRecordKeyResolver
    implements Strata.Resolver, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public Object resolve(Object txn, Object object)
        {
            return ((IndexRecord) object).key;
        }
    }

    private final static class IndexRecordByKey
    implements Strata.Criteria
    {
        private final Long key;

        public IndexRecordByKey(Long key)
        {
            this.key = key;
        }

        public int partialMatch(Object object)
        {
            return key.compareTo(((IndexRecord) object).key);
        }

        public boolean exactMatch(Object object)
        {
            return key.equals(((IndexRecord) object).key);
        }

        public Object getObject()
        {
            return null;
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
            return (SizeOf.INTEGER + SizeOf.LONG) * size;
        }

        public void write(ByteBuffer bytes, Object object)
        {
            if (object == null)
            {
                for (int i = 0; i < size; i++)
                {
                    bytes.putInt(0);
                    bytes.putLong(0L);
                }
            }
            else
            {
                RecordReference[] references = (RecordReference[]) object;
                for (int i = 0; i < size; i++)
                {
                    bytes.putInt(references[i].getBagKey().intValue());
                    bytes.putLong(references[i].getKey().longValue());
                }
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
            RecordReference[] keys = new RecordReference[size];
            for (int i = 0; i < size; i++)
            {
                keys[i] = new RecordReference(new Integer(bytes.getInt()), new Long(bytes.getLong()));
            }
            return keys[0].getBagKey().intValue() == 0 ? null : keys;
        }
    }

    private final static class JoinComparison
    implements Strata.Comparison, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final int size;

        public JoinComparison(int size)
        {
            this.size = size;
        }

        public int partialMatch(Object criteria, Object stored)
        {
            RecordReference[] left = (RecordReference[]) criteria;
            RecordReference[] right = (RecordReference[]) stored;
            for (int i = 0; i < size; i++)
            {
                int compare = left[i].compareTo(right[i]);
                if (compare == 0)
                {
                    continue;
                }
                return compare;
            }
            return 0;
        }

        public boolean exactMatch(Object criteria, Object stored)
        {
            RecordReference[] left = (RecordReference[]) criteria;
            RecordReference[] right = (RecordReference[]) stored;
            if (left.length != right.length)
            {
                throw new IllegalArgumentException();
            }
            for (int i = 0; i < left.length; i++)
            {
                if (left[i].equals(right[i]))
                {
                    return false;
                }
            }
            return true;
        }
    }

    public final static class Creator
    {
        private final Map mapOfBagCreators = new HashMap();

        private final Map mapOfRelationshipCreators = new HashMap();

        public Storage create(File file)
        {
            Bento.Creator newBento = new Bento.Creator();
            newBento.addStaticPage(MONKEY_URI, Bento.ADDRESS_SIZE * 2);
            Bento bento = newBento.create(file);
            Bento.Mutator mutator = bento.mutate(bento.newNullJournal());

            int count = 0;
            Map mapOfBags = new HashMap();
            List listOfBagNames = new ArrayList();
            Iterator bags = mapOfBagCreators.entrySet().iterator();
            while (bags.hasNext())
            {
                Map.Entry entry = (Map.Entry) bags.next();
                String name = (String) entry.getKey();
                BagCreator newBag = (BagCreator) entry.getValue();

                Strata strata = newBag.create(mutator);
                mapOfBags.put(name, new BagIndex(strata, new Integer(++count)));
                listOfBagNames.add(name);
            }

            Bento.OutputStream allocation = new Bento.OutputStream(mutator);
            try
            {
                ObjectOutputStream out = new ObjectOutputStream(allocation);
                out.writeObject(mapOfBags);
                out.writeObject(listOfBagNames);
            }
            catch (IOException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);

            }
            Bento.Address addressOfBags = allocation.allocate(false);

            Map mapOfJoins = new HashMap();
            Iterator relationships = mapOfRelationshipCreators.entrySet().iterator();
            while (relationships.hasNext())
            {
                Map.Entry entry = (Map.Entry) relationships.next();
                String name = (String) entry.getKey();
                JoinCreator newJoin = (JoinCreator) entry.getValue();
                Strata strata = newJoin.create(mutator);
                mapOfJoins.put(name, strata);
            }
            allocation = new Bento.OutputStream(mutator);
            try
            {
                ObjectOutputStream out = new ObjectOutputStream(allocation);
                out.writeObject(mapOfJoins);
            }
            catch (IOException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);

            }
            Bento.Address addressOfJoins = allocation.allocate(false);

            Bento.Block block = mutator.load(bento.getStaticAddress(MONKEY_URI));

            ByteBuffer data = block.toByteBuffer();

            data.putLong(addressOfBags.getPosition());
            data.putInt(addressOfBags.getBlockSize());

            data.putLong(addressOfJoins.getPosition());
            data.putInt(addressOfJoins.getBlockSize());

            block.write();

            mutator.getJournal().commit();
            bento.close();

            return new Storage.Opener().open(file);
        }

        public BagCreator newBag(String name)
        {
            BagCreator newBag = new BagCreator();
            mapOfBagCreators.put(name, newBag);
            return newBag;
        }

        public JoinCreator newJoin(String name)
        {
            JoinCreator newRelationship = new JoinCreator();
            mapOfRelationshipCreators.put(name, newRelationship);
            return newRelationship;
        }
    }

    public final static class Opener
    {
        public Storage open(File file)
        {
            Bento bento = new Bento.Opener().open(file);
            Bento.Mutator mutator = bento.mutate();
            Bento.Block block = mutator.load(bento.getStaticAddress(MONKEY_URI));
            ByteBuffer data = block.toByteBuffer();
            Bento.Address addressOfBags = new Bento.Address(data.getLong(), data.getInt());
            Bento.Address addressOfJoins = new Bento.Address(data.getLong(), data.getInt());
            Map mapOfBagIndices = null;
            List listOfBagNames = null;
            ObjectInputStream objects;
            try
            {
                objects = new ObjectInputStream(new ByteBufferInputStream(mutator.load(addressOfBags).toByteBuffer(), false));
                mapOfBagIndices = (Map) objects.readObject();
                listOfBagNames = (List) objects.readObject();
            }
            catch (IOException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }
            catch (ClassNotFoundException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }

            Map mapOfJoinIndices;
            try
            {
                objects = new ObjectInputStream(new ByteBufferInputStream(mutator.load(addressOfJoins).toByteBuffer(), false));
                mapOfJoinIndices = (Map) objects.readObject();
            }
            catch (IOException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }
            catch (ClassNotFoundException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }

            return new Storage(file, bento, listOfBagNames, mapOfBagIndices, mapOfJoinIndices);
        }
    }

    public final static class BagCreator
    {
        private final Map mapOfIndicies;

        public BagCreator()
        {
            this.mapOfIndicies = new HashMap();
        }

        public IndexCreator newIndex(String name)
        {
            IndexCreator newIndex = new IndexCreator();
            mapOfIndicies.put(name, newIndex);
            return newIndex;
        }

        public void addIndex(String name, Strata.FieldExtractor fields)
        {
            IndexCreator newIndex = newIndex(name);
            newIndex.setFields(fields);
        }

        public Strata create(Bento.Mutator mutator)
        {
            BentoStorage.Creator newBentoStorage = new BentoStorage.Creator();
            newBentoStorage.setWriter(new IndexRecordWriter());
            newBentoStorage.setReader(new IndexRecordReader());

            Strata.Creator newStrata = new Strata.Creator();

            newStrata.setStorage(newBentoStorage.create());
            newStrata.setCriteriaServer(new Strata.ComplexCriteriaServer(new IndexRecordKeyResolver(), new Strata.BasicComparison()));

            return newStrata.create(BentoStorage.txn(mutator));
        }
    }

    public final static class JoinCreator
    {
        private int size = 2;

        private int indexSize = 1;

        public Strata create(Bento.Mutator mutator)
        {
            BentoStorage.Creator newBentoStorage = new BentoStorage.Creator();
            newBentoStorage.setWriter(new JoinWriter(size));
            newBentoStorage.setReader(new JoinReader(size));

            Strata.Creator newStrata = new Strata.Creator();

            newStrata.setStorage(newBentoStorage.create());
            newStrata.setCriteriaServer(new Strata.ComplexCriteriaServer(new Strata.BasicResolver(), new JoinComparison(indexSize)));

            return newStrata.create(BentoStorage.txn(mutator));
        }

        public void setSize(int size)
        {
            this.size = size;
        }

        public void setIndexSize(int indexSize)
        {
            this.indexSize = indexSize;
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

    private final static class BagIndex
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Strata strata;

        private final Integer bagIndex;

        public BagIndex(Strata strata, Integer bagIndex)
        {
            this.strata = strata;
            this.bagIndex = bagIndex;
        }
    }

    public final class Bag
    {
        private final Integer key;

        private final Mutator mutator;

        private final Strata.Query query;

        public final Map mapOfObjects = new LinkedHashMap();

        public Bag(Integer key, Mutator mutator, Strata.Query query)
        {
            this.key = key;
            this.mutator = mutator;
            this.query = query;
        }

        public Integer getKey()
        {
            return key;
        }

        public Record add(Object object)
        {
            Record record = new Record(new RecordReference(key, new Long(++identifier)), object);
            Bento.OutputStream allocation = new Bento.OutputStream(mutator.mutator);
            try
            {
                ObjectOutputStream out = new ObjectOutputStream(allocation);
                out.writeObject(record);
            }
            catch (IOException e)
            {
                throw new AsinineCheckedExceptionThatIsEntirelyImpossible(e);
            }
            Bento.Address address = allocation.allocate(false);
            query.insert(new IndexRecord(record.getReference().getKey(), address));
            return record;
        }

        public Record get(Long key)
        {
            Strata.Cursor cursor = query.find(new IndexRecordByKey(key));
            if (cursor.isEmpty())
            {
                return null;
            }
            IndexRecord record = (IndexRecord) cursor.next();
            return (Record) new IndexRecordObjectResolver().resolve(mutator, record);
        }
    }

    public final static class Join
    {
        private final Mutator mutator;

        private final Strata.Query query;

        public Join(Mutator mutator, Strata.Query query)
        {
            this.mutator = mutator;
            this.query = query;
        }

        public void add(Record left, Record right)
        {
            add(new RecordReference[] { left.getReference(), right.getReference() });
        }

        public void add(RecordReference[] references)
        {
            // FIXME Only unique.
            query.insert(references);
        }

        public Strata.Cursor find(Record record)
        {
            return find(new RecordReference[] { record.getReference() });
        }

        public Strata.Cursor find(RecordReference[] references)
        {
            return new JoinCursor(mutator, query.find(references));
        }
    }

    private final static class JoinCursor
    implements Strata.Cursor
    {
        private final Strata.Cursor cursor;

        private final Storage.Mutator mutator;

        public JoinCursor(Storage.Mutator mutator, Strata.Cursor cursor)
        {
            this.mutator = mutator;
            this.cursor = cursor;
        }

        public boolean hasNext()
        {
            return cursor.hasNext();
        }

        public boolean isEmpty()
        {
            return cursor.isEmpty();
        }

        public boolean isForward()
        {
            return cursor.isForward();
        }

        public Cursor newCursor()
        {
            return new JoinCursor(mutator, cursor.newCursor());
        }

        public Cursor reverse()
        {
            return new JoinCursor(mutator, cursor.reverse());
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public Object next()
        {
            RecordReference[] recordReferences = (RecordReference[]) cursor.next();
            Record[] records = new Record[recordReferences.length];
            for (int i = 0; i < recordReferences.length; i++)
            {
                records[i] = mutator.get(recordReferences[i]);
            }
            return records;
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

    private final Map mapOfBagIndices;

    private final List listOfBagNames;

    private final Map mapOfJoinIndices;

    private final Bento bento;

    private long identifier;

    public Storage(File file, Bento bento, List listOfBagNames, Map mapOfBagIndicies, Map mapOfJoinIndices)
    {
        this.identifier = 0;
        this.listOfBagNames = listOfBagNames;
        this.mapOfBagIndices = mapOfBagIndicies;
        this.mapOfJoinIndices = mapOfJoinIndices;
        this.bento = bento;
    }

    public void close()
    {
        bento.close();
    }

    public Mutator mutate()
    {
        return new Mutator(bento.mutate());
    }

    public class Mutator
    implements MutatorServer
    {
        private final Bento.Mutator mutator;

        private final Map mapOfBags;

        private final Map mapOfObjects = new HashMap();

        private final Map mapOfJoins;

        public Mutator(Bento.Mutator mutator)
        {
            this.mutator = mutator;
            this.mapOfBags = new HashMap();
            this.mapOfJoins = new HashMap();
        }

        public Record get(RecordReference reference)
        {
            return getBag(reference.getBagKey()).get(reference.getKey());
        }

        public Bag getBag(Integer key)
        {
            return getBag((String) listOfBagNames.get(key.intValue() - 1));
        }

        public Bag getBag(String name)
        {
            Bag bag = (Bag) mapOfBags.get(name);
            if (bag == null)
            {
                BagIndex bagIndex = (BagIndex) mapOfBagIndices.get(name);
                bag = new Bag(bagIndex.bagIndex, this, bagIndex.strata.query(this));
                mapOfBags.put(name, bag);
            }
            return bag;
        }

        public Join getJoin(String name)
        {
            Join join = (Join) mapOfJoins.get(name);
            if (join == null)
            {
                Strata relationshipIndex = (Strata) mapOfJoinIndices.get(name);
                join = new Join(this, relationshipIndex.query(this));
                mapOfJoins.put(name, join);
            }
            return join;
        }

        public void commit()
        {
            if (mapOfBags.size() != 0 || mapOfJoins.size() != 0)
            {
                Iterator bags = mapOfBags.values().iterator();
                while (bags.hasNext())
                {
                    Bag bag = (Bag) bags.next();
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
            }
        }

        public void rollback()
        {
            if (mapOfBags.size() != 0 || mapOfJoins.size() != 0)
            {
                Iterator bags = mapOfBags.values().iterator();
                while (bags.hasNext())
                {
                    Bag bag = (Bag) bags.next();
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
            }
        }

        public Bento.Mutator getMutator()
        {
            return mutator;
        }
    }

    public final static class ObjectInputStream
    extends java.io.ObjectInputStream
    {
        public ObjectInputStream(InputStream in) throws IOException
        {
            super(in);
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */