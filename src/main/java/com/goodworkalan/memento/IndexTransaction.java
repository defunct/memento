package com.goodworkalan.memento;

public final class IndexTransaction<Item, Fields, X>
{
    public final X mutator;

    public final Bin<Item> bin;

    public final IndexSchema<Item, Fields> schema;

    public IndexTransaction(X mutator, Bin<Item> bin, IndexSchema<Item, Fields> schema)
    {
        this.mutator = mutator;
        this.bin = bin;
        this.schema = schema;
    }

    public X getMutator()
    {
        return mutator;
    }

    public Fields index(long key, long version)
    {
        return schema.extractor.index(bin.get(schema.io, key, version).getItem());
    }

    public Box<Item> getBag(BinRecord record)
    {
        return bin.get(schema.io, record.key);
    }

    public boolean isDeleted(BinRecord record)
    {
        return record.version != bin.get(schema.io, record.key).getVersion();
    }
}