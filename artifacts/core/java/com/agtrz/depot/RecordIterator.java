/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.util.Iterator;

public class RecordIterator
implements Iterator
{
    private final Iterator iterator;

    public RecordIterator(Iterator iterator)
    {
        this.iterator = iterator;
    }

    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    public Object next()
    {
        return ((Storage.Record) iterator.next()).getObject();
    }

    public void remove()
    {
        iterator.remove();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */