package com.goodworkalan.memento;

public class BinKeyComparable implements Comparable<BinRecord>
{
    private final Long key;
    
    public BinKeyComparable(long key)
    {
        this.key = key;
    }

    public int compareTo(BinRecord o)
    {
        return key.compareTo(o.key);
    }
}
