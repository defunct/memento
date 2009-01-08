package com.goodworkalan.memento;

@SuppressWarnings("unchecked")
public class Ordered implements Comparable<Ordered>
{
    private Comparable[] comparables;
    
    public Ordered(Comparable<?>...comparables)
    {
        this.comparables = comparables;
    }
    
    public int compareTo(Ordered ordered)
    {
        if (ordered == this)
        {
            return 0;
        }
        int min = Math.min(comparables.length, ordered.comparables.length);
        for (int i = 0; i < min; i++)
        {
            if (comparables[i] == null)
            {
                if (ordered.comparables[i] != null)
                {
                    return -1;
                }
            }
            else if (ordered.comparables[i] == null)
            {
                return 1;
            }
            else
            {
                int compare = comparables[i].compareTo(ordered.comparables[i]);
                if (compare != 0)
                {
                    return compare;
                }
            }
        }
        return 0;
    }
}
