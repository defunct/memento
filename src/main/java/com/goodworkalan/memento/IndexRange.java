package com.goodworkalan.memento;

import java.util.Iterator;


public class IndexRange
implements Iterator<Object>
{
    private int count;

    private final int limit;

    private final Cursor cursor;

    public Range(Cursor cursor, int offset, int limit)
    {
        while (offset != 0 && cursor.hasNext())
        {
            cursor.next();
            offset--;
        }
        this.limit = limit;
        this.cursor = cursor;
    }

    public boolean hasNext()
    {
        return count < limit && cursor.hasNext();
    }

    public Bag nextBag()
    {
        Bag bag = cursor.nextBag();
        count++;
        return bag;
    }

    public Object next()
    {
        Bag bag = nextBag();
        if (!hasNext())
        {
            cursor.release();
        }
        return bag.getObject();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void release()
    {
        cursor.release();
    }
}