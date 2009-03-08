package com.goodworkalan.memento;

import static com.goodworkalan.memento.IndexSchema.EXTRACTOR;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.ExtractorComparableFactory;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Schema;

// FIXME Vacuum.
public final class IndexMutator<T, F extends Comparable<F>>
{
    private final IndexSchema<T, F> schema;

    private final Query<IndexRecord> isolation;

    public IndexMutator(BinTable bins, IndexSchema<T, F> schema)
    {
        this.schema = schema;
        this.isolation = newIsolation(schema, bins);
    }

    private static <T, F extends Comparable<F>> Query<IndexRecord> newIsolation(IndexSchema<T, F> indexSchema, BinTable bins)
    {
        Schema<IndexRecord> schema = new Schema<IndexRecord>();

        schema.setInnerCapacity(7);
        schema.setLeafCapacity(7);
        schema.setComparableFactory(new ExtractorComparableFactory<IndexRecord, F>(new IndexExtractor<T, F>(indexSchema.getItem(), indexSchema.getIndex())));

        // FIXME Ignored.
        Stash stash = new Stash();
        stash.put(EXTRACTOR, BinTable.class, bins);

        return schema.inMemory(new Ilk<IndexRecord>() { }).query();
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
        Cursor<IndexRecord> found = isolation.find(null /*schema.getIndexer().index(bin.box(box.getKey(), previous).getItem())*/);
        while (found.hasNext())
        {
            IndexRecord record = found.next();
            if (record.key.equals(box.getKey()) && record.version == previous)
            {
                found.release();
                isolation.remove(isolation.newComparable(record));
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
        Stash stash = Fossil.initialize(new Stash(), mutator);
        return new IndexCursor<T,F>(schema, schema.getStrata().query(stash).find(null /* fields */), isolation.find(null /* fields */), stash, limit);
    }

    IndexCursor<T,F> first(Snapshot snapshot, Mutator mutator, Bin<T> bin)
    {
        // TODO Setup stash.
        Stash stash = Fossil.initialize(new Stash(), mutator);
        return new IndexCursor<T, F>(schema, schema.getStrata().query(stash).first(), isolation.first(), stash, false);
    }

    void commit(Snapshot snapshot, Mutator mutator, Bin<T> bin)
    {
        Query<IndexRecord> queryOfStored = schema.getStrata().query(Fossil.initialize(new Stash(), mutator));
        Cursor<IndexRecord> isolated = isolation.first();
        try
        {
            while (isolated.hasNext())
            {
                IndexRecord record = isolated.next();
                queryOfStored.add(record);
                if (schema.isUnique())
                {
//                    Box<T> box = bin.box(record.key, record.version);
//                    F fields = schema.getIndexer().index(box.getItem());
                    /*if ( schema.notNull || !hasNulls(fields) )
                    {*/
                        Cursor<IndexRecord> found = queryOfStored.find(null /* fields */);
                        try
                        {
                            while (found.hasNext())
                            {
                                IndexRecord existing = found.next();
                                if (existing.key.equals(record.key) && existing.version.equals(record.version))
                                {
                                    break;
                                }
                                else if (!snapshot.isVisible(existing.version))
                                {
                                    throw new MementoException(110);
                                }
                            }
                        }
                        finally
                        {
                            found.release();
                        }
                   // }
                }
            }
        }
        finally
        {
            isolated.release();
        }
    }
}