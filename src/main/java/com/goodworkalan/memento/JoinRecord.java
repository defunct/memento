package com.goodworkalan.memento;

final class JoinRecord
{
    public final long[] keys;

    public final long version;

    public final boolean deleted;

    public JoinRecord(long[] keys, long version, boolean deleted)
    {
        this.keys = keys;
        this.version = version;
        this.deleted = deleted;
    }

    public boolean equals(Object object)
    {
        if (object instanceof JoinRecord)
        {
            JoinRecord record = (JoinRecord) object;
            long[] left = keys;
            long[] right = record.keys;
            if (left.length != right.length)
            {
                return false;
            }
            for (int i = 0; i < left.length; i++)
            {
                if (left[i] != right[i])
                {
                    return false;
                }
            }
            return version == record.version && deleted == record.deleted;
        }
        return false;
    }

    public int hashCode()
    {
        int hashCode = 1;
        for (int i = 0; i < keys.length; i++)
        {
            hashCode = hashCode * 37 + new Long(keys[i]).hashCode();
        }
        hashCode = hashCode * 37 + new Long(version).hashCode();
        hashCode = hashCode * 37 + (deleted ? 1 : 0);
        return hashCode;
    }
}