package com.goodworkalan.memento;

import com.goodworkalan.ilk.Ilk;

// TODO Document.
public class Index<T>
{
    // TODO Document.
    private final Ilk<T> ilk;
    
    // TODO Document.
    private final String name;

    // TODO Document.
    Index(Ilk<T> ilk, String name)
    {
        this.ilk = ilk;
        this.name = name;
    }
    
    // TODO Document.
    public Ilk<T> getIlk()
    {
        return ilk;
    }
    
    // TODO Document.
    public String getName()
    {
        return name;
    }
    
    // TODO Document.
    public boolean equals(Object object)
    {
        if (object instanceof Index)
        {
            Index<?> index = (Index<?>) object;
            return ilk.key.equals(index.ilk.key) && name.equals(index.name);
        }
        return false;
    }
    
    // TODO Document.
    @Override
    public int hashCode()
    {
        int hashCode = 1999;
        hashCode = hashCode * 37 + ilk.key.hashCode();
        hashCode = hashCode * 37 + name.hashCode();
        return hashCode;
    }
}
