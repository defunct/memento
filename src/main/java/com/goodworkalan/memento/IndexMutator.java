package com.goodworkalan.memento;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.Strata;
import com.goodworkalan.strata.Stratas;

// FIXME Vacuum.
public final class IndexMutator<T, F>
{
    private final IndexSchema<T, F> schema;

    private final Query<IndexRecord, Ordered> isolation;

    public IndexMutator(IndexSchema<T, F> schema)
    {
        this.schema = schema;
        this.isolation = newIsolation();
    }

    private static Query<IndexRecord, Ordered> newIsolation()
    {
        Schema<IndexRecord, Ordered> newStrata = Stratas.newInMemorySchema();

        newStrata.setFieldCaching(true);
        newStrata.setExtractor(new IndexExtractor<T, F>());

        return newStrata.newTransaction(null);
    }

    public void add(Snapshot snapshot, Mutator mutator, Bin<Item> bin, Box<Item> box)
    {
        IndexTransaction txn = new IndexTransaction(mutator, bin, schema);
        Fields fields = schema.extractor.index(box.getItem());
        // Need to push not null into indexer. 
//        if (schema.notNull && Depot.hasNulls(fields))
//        {
//            throw new Error("Not null violation.", Depot.NOT_NULL_VIOLATION_ERROR);
//        }
        if (schema.unique)// && (schema.notNull || !hasNulls(fields)))
        {
            Iterator<Object> found = find(snapshot, mutator, bin, fields, true);
            if (found.hasNext())
            {
                found.next(); // Release locks.
                throw new Error("Unique index constraint violation.", Depot.UNIQUE_CONSTRAINT_VIOLATION_ERROR).put("bin", bin.getName());
            }
        }
        Query query = isolation.query(mutator);
        IndexRecord record = new IndexRecord(bag.getKey(), bag.getVersion());
        query.add(record);
        query.flush();
    }

    public void update(Snapshot snapshot, Mutator mutator, Bin bin, Bag bag, Long previous)
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

    public void remove(Mutator mutator, Bin bin, Long key, Long version)
    {
        IndexTransaction txn = new Transaction(mutator, bin, schema);
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

 
  

   

 
   

}