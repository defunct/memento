package com.goodworkalan.memento;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.goodworkalan.ilk.Ilk;

public class Link
{
    private final String name;
    
    private final List<Ilk.Key> ilkKeys = new ArrayList<Ilk.Key>();

    public Link()
    {
        this.name = "";
    }

    public Link(String name)
    {
        this.name = name;
    }
    
    public <T> Link bin(Ilk<T> ilk)
    {
        ilkKeys.add(ilk.key);
        return this;
    }
    
    public <T> Link bin(Class<T> meta)
    {
        ilkKeys.add(new Ilk.Key(meta));
        return this;
    }
    
    public List<Ilk.Key> getIlkKeys()
    {
        return Collections.unmodifiableList(ilkKeys);
    }
    
    public int size()
    {
        return ilkKeys.size();
    }
    
    @Override
    public boolean equals(Object object)
    {
        if (object instanceof Link)
        {
            Link link = (Link) object;
            return name.equals(link.name) && ilkKeys.equals(link.ilkKeys);
        }
        return false;
    }
    
    @Override
    public int hashCode()
    {
        int hashCode = 17;
        hashCode = hashCode * 37 + name.hashCode();
        hashCode = hashCode * 37 + name.hashCode();
        return hashCode;
    }

    final boolean valid(BinSchemaTable binSchemas)
    {
        for (Ilk.Key key : ilkKeys)
        {
            if (!binSchemas.has(key))
            {
                return false;
            }
        }
        return true;
    }
}
