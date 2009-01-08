package com.goodworkalan.memento;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.agtrz.depot.Depot;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Strata;
import com.goodworkalan.strata.Tree;

// FIXME Vacuum.
public final static class Index
{
    private final IndexSchema schema;

    private final Tree<IndexRecord, Mutator> isolation;

    public Index(IndexSchema schema)
    {
        this.schema = schema;
        this.isolation = newIsolation();
    }

    private static Tree<IndexRecord, Mutator> newIsolation()
    {
        com.goodworkalan.strata.Schema<T, X>Strata.newInMemorySchema();
        Strata.Schema creator = new Strata.Schema();

        creator.setCacheFields(true);
        creator.setFieldExtractor(new Extractor());
        creator.setStorage(new ArrayListStorage.Schema());

        return creator.newStrata(null);
    }

    public void add(Snapshot snapshot, Mutator mutator, Bin bin, Bag bag)
    {
        IndexTransaction txn = new IndexTransaction(mutator, bin, schema);
        Comparable<?>[] fields = schema.extractor.getFields(bag.getObject());
        if (schema.notNull && Depot.hasNulls(fields))
        {
            throw new Error("Not null violation.", Depot.NOT_NULL_VIOLATION_ERROR);
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