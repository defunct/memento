package com.agtrz.depot;

final class BinRecord
{
    public final long key;

    public final long version;

    public final long address;

    public BinRecord(long key, long version, long address)
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
            return key == record.key && version == record.version;
        }
        return false;
    }

    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + ((Long) key).hashCode();
        hashCode = hashCode * 37 + ((Long) version).hashCode();
        return hashCode;
    }
}