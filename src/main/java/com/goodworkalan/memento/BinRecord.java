package com.goodworkalan.memento;

// TODO Document.
final class BinRecord
{
    // TODO Document.
    public final long key;

    // TODO Document.
    public final long version;

    // TODO Document.
    public final long address;

    // TODO Document.
    public BinRecord(long key, long version, long address)
    {
        this.key = key;
        this.version = version;
        this.address = address;
    }

    // TODO Document.
    public boolean equals(Object object)
    {
        if (object instanceof BinRecord)
        {
            BinRecord record = (BinRecord) object;
            return key == record.key && version == record.version;
        }
        return false;
    }

    // TODO Document.
    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + ((Long) key).hashCode();
        hashCode = hashCode * 37 + ((Long) version).hashCode();
        return hashCode;
    }
}