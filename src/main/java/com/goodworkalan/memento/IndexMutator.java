package com.goodworkalan.memento;

import static com.goodworkalan.memento.IndexSchema.EXTRACTOR;

import java.util.Iterator;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.InMemoryStorageBuilder;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.Strata;
import com.goodworkalan.strata.Stratas;

// FIXME Vacuum.
public final class IndexMutator<T, F extends Comparable<F>>
{
    private final IndexSchema<T, F> schema;

    private final Query<IndexRecord, F> isolation;

    public IndexMutator(BinTable bins, IndexSchema<T, F> schema)
    {
        this.schema = schema;
        this.isolation = newIsolation(schema, bins);
    }

    private static <T, F extends Comparable<F>> Query<IndexRecord, F> newIsolation(IndexSchema<T, F> indexSchema, BinTable bins)
    {
        Schema<IndexRecord, F> newStrata = Stratas.newInMemorySchema();

        newStrata.setFieldCaching(true);
        newStrata.setExtractor(new IndexExtractor<T, F>(indexSchema.getItem(), indexSchema.getIndex()));

        Stash stash = new Stash();
        stash.put(EXTRACTOR, BinTable.class, bins);
        return newStrata.create(stash, new InMemoryStorageBuilder<IndexRecord, F>());
    }

    public void add(Snapshot snapshot, Mutator mutator, Bin<T> bin, Box<T> box)
    {
        F fields = schema.getIndexer().index(box.getItem());
        // Need to push not null into indexer. 
//        if (schema.notNull && Depot.hasNulls(fields))
//        {
//            throw new Error("Not null violation.", Depot.NOT_NULL_VIOLATION_ERROR);
//        }
        if (schema.isUnique())// && (schema.notNull || !hasNulls(fields)))
        {
            IndexCursor<T, F> exists = find(snapshot, mutator, bin, fields, true);
            if (exists.hasNext())
            {
                exists.next(); // Release locks.
                throw new MementoException(116);
            }
        }
        IndexRecord record = new IndexRecord(box.getKey(), box.getVersion());
        isolation.add(record);
    }

    public void update(Snapshot snapshot, Mutator mutator, Bin<T> bin, Box<T> box, long previous)
    {
        Cursor<IndexRecord> found = isolation.find(schema.getIndexer().index(bin.box(box.getKey(), previous).getItem()));
        while (found.hasNext())
        {
            IndexRecord record = found.next();
            if (record.key.equals(box.getKey()) && record.version == previous)
            {
                found.release();
                isolation.remove(isolation.extract(record));
                break;
            }
        }
        found.release();
        F fields = schema.getIndexer().index(box.getItem());
        // TODO Not null done with special version of ordered.
//        if (schema.notNull && hasNulls(fields))
//        {
//            throw new Error("Not null violation.", NOT_NULL_VIOLATION_ERROR);
//        }
        if (schema.isUnique() ) //&& (schema.notNull || !hasNulls(fields)))
        {
            IndexCursor<T, F> exists = find(snapshot, mutator, bin, fields, true);
            if (exists.hasNext())
            {
                Box<T> existing = exists.nextBox();
                if (existing.getKey() != box.getKey())
                {
                    throw new MementoException(116);
                }
            }
        }
        isolation.add(new IndexRecord(box.getKey(), box.getVersion()));
    }

    public void remove(Mutator mutator, Bin<T> bin, Long key, Long version)
    {
        // FIXME Was not happening in isolation!
//        IndexTransaction txn = new Transaction(mutator, bin, schema);
//        final Box<T> box = bin.box(key, version);
//        F fields = schema.getIndexer().index(box.getItem());
//        schema.getStrata().query(txn).remove(fields, new Strata.Deletable()
//        {
//            public boolean deletable(Object object)
//            {
//                Record record = (Record) object;
//                return record.key.equals(bag.getKey()) && record.version.equals(bag.getVersion());
//            }
//        });
    }

    IndexCursor<T, F> find(Snapshot snapshot, Mutator mutator, Bin<T> bin, F fields, boolean limit)
    {
        // TODO Setup stash.
        return new IndexCursor<T,F>(schema.getStrata().query(txn).find(fields), isolation.query(txn).find(fields), txn, fields, limit);
    }

    IndexCursor<T,F> first(Snapshot snapshot, Mutator mutator, Bin<T> bin)
    {
        // TODO Setup stash.
        return new IndexCursor<T, F>(schema.getStrata().query(txn).first(), isolation.query(txn).first(), txn, 0, false);
    }

    void commit(Snapshot snapshot, Mutator mutator, Bin<T> bin)
    {
        // TODO Setup stash.
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