package com.goodworkalan.memento;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Link
{
    private final String name;
    
    private final List<Item<?>> items = new ArrayList<Item<?>>();

    public Link()
    {
        this.name = "";
    }

    public Link(String name)
    {
        this.name = name;
    }
    
    public <T> Link bin(Item<T> item)
    {
        items.add(item);
        return this;
    }
    
    public <T> Link bin(Class<T> itemClass)
    {
        items.add(new Item<T>(itemClass) {});
        return this;
    }
    
    public List<Item<?>> getItems()
    {
        return Collections.unmodifiableList(items);
    }
    
    public int size()
    {
        return items.size();
    }
    
    @Override
    public boolean equals(Object object)
    {
        if (object instanceof Link)
        {
            Link link = (Link) object;
            return name.equals(link.name) && items.equals(link.items);
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
        for (Item<?> item : items)
        {
            if (!binSchemas.has(item))
            {
                return false;
            }
        }
        return true;
    }
}
