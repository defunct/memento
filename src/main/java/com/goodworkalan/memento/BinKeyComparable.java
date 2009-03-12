package com.goodworkalan.memento;

// TODO Document.
public class BinKeyComparable implements Comparable<BinRecord>
{
    // TODO Document.
    private final Long key;
    
    // TODO Document.
    public BinKeyComparable(long key)
    {
        this.key = key;
    }

    // TODO Document.
    public int compareTo(BinRecord o)
    {
        return key.compareTo(o.key);
    }
}
