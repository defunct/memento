package com.goodworkalan.memento;

// TODO Document.
public class SnapshotVersionComparable implements Comparable<SnapshotRecord>
{
    // TODO Document.
    private final Long version;
    
    // TODO Document.
    public SnapshotVersionComparable(long version)
    {
        this.version = version;
    }
    
    // TODO Document.
    public int compareTo(SnapshotRecord o)
    {
        return version.compareTo(o.version);
    }
}
