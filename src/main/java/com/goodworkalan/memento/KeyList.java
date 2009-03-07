package com.goodworkalan.memento;

import java.util.ArrayList;

public class KeyList extends ArrayList<Long> implements Comparable<KeyList>
{
    private static final long serialVersionUID = 1L;

    public KeyList(int size)
    {
        super(size);
    }

    public int compareTo(KeyList o)
    {
        int stop = Math.min(size(), o.size());
        for (int i = 0; i < stop; i++)
        {
            int compare = get(i).compareTo(o.get(i));
            if (compare != 0)
            {
                return compare;
            }
        }
        return size() - o.size();
    }
}
