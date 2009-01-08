package com.goodworkalan.memento;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;

public final static class IndexTransaction
{
    public final Mutator mutator;

    public final Bin bin;

    public final IndexSchema schema;

    public IndexTransaction(Mutator mutator, Bin bin, IndexSchema schema)
    {
        this.mutator = mutator;
        this.bin = bin;
        this.schema = schema;
    }

    public Mutator getMutator()
    {
        return mutator;
    }

    public Comparable<?>[] getFields(Long key, Long version)
    {
        return schema.extractor.getFields(bin.get(schema.unmarshaller, key, version).getObject());
    }

    public Bag getBag(BinRecord record)
    {
        return bin.get(schema.unmarshaller, record.key);
    }

    public boolean isDeleted(BinRecord record)
    {
        return record.version != bin.get(schema.unmarshaller, record.key).getVersion();
    }
}