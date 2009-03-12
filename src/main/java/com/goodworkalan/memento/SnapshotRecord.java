package com.goodworkalan.memento;

// TODO Document.
public class SnapshotRecord
{
    // TODO Document.
    public final long version;

    // TODO Document.
    public final int state;

    // TODO Document.
    public SnapshotRecord(long version, int state)
    {
        this.version = version;
        this.state = state;
    }

    // TODO Document.
    public boolean equals(Object object)
    {
        if (object instanceof SnapshotRecord)
        {
            SnapshotRecord record = (SnapshotRecord) object;
            return version == record.version && state == record.state;
        }
        return false;
    }

    // TODO Document.
    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + new Long(version).hashCode();
        hashCode = hashCode * 37 + new Integer(state).hashCode();
        return hashCode;
    }
}
