package com.goodworkalan.memento;

public class SnapshotVersionComparable implements Comparable<SnapshotRecord>
{
    private final Long version;
    
    public SnapshotVersionComparable(long version)
    {
        this.version = version;
    }
    
    public int compareTo(SnapshotRecord o)
    {
        return version.compareTo(o.version);
    }
}
