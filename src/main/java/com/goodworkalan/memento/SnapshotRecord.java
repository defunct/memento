package com.goodworkalan.memento;

public class SnapshotRecord
{
    public final long version;

    public final int state;

    public SnapshotRecord(long version, int state)
    {
        this.version = version;
        this.state = state;
    }

    public boolean equals(Object object)
    {
        if (object instanceof SnapshotRecord)
        {
            SnapshotRecord record = (SnapshotRecord) object;
            return version == record.version && state == record.state;
        }
        return false;
    }

    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + new Long(version).hashCode();
        hashCode = hashCode * 37 + new Integer(state).hashCode();
        return hashCode;
    }
}
