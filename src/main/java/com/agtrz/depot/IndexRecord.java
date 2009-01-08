package com.agtrz.depot;


public class IndexRecord
{
    public final Long key;

    public final Long version;

    public IndexRecord(Long key, Long version)
    {
        this.key = key;
        this.version = version;
    }

    public boolean equals(Object object)
    {
        if (object instanceof IndexRecord)
        {
            IndexRecord record = (IndexRecord) object;
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