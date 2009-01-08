package com.goodworkalan.memento;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
 
public class WeakIdentityLookup
{
    private final Map<WeakKey, Long> map;
    
    private final ReferenceQueue<Object> queue;
    
    public WeakIdentityLookup()
    {
        this.map = new HashMap<WeakKey, Long>();
        this.queue = new ReferenceQueue<Object>();
    }
    
    private void collect()
    {
        WeakKey key;
        while ((key = (WeakKey) queue.poll()) != null) 
        {
            map.remove(key);
        }
    }
    
    public Long get(Object item)
    {
        collect();
        return map.get(item);
    }
    
    public void put(Object item, Long key)
    {
        collect();
        map.put(new WeakKey(item, queue), key);
    }
    
    public void clear()
    {
        for (WeakKey key : map.keySet())
        {
            key.enqueue();
        }
        collect();
    }
    
    private final static class SearchKey
    {
        public final Object referant;
        
        public SearchKey(Object referant)
        {
            this.referant = referant;
        }
        
        @Override
        public boolean equals(Object object)
        {
            return ((WeakKey) object).equals(this);
        }
        
        @Override
        public int hashCode()
        {
            return referant.hashCode();
        }
    }
    
    private final static class WeakKey extends WeakReference<Object> 
    {
        private final int hashCode;
        
        public WeakKey(Object referant, ReferenceQueue<Object> queue)
        {
            super(referant, queue);
            this.hashCode = referant.hashCode();
        }

        @Override
        public boolean equals(Object object)
        {
            if (object == this)
            {
                return true;
            }
            if (object instanceof WeakKey)
            {
                WeakKey key = (WeakKey) object;
                return get() != null && get() == key.get();
            }
            return ((SearchKey) object).referant == get();
        }
        
        @Override
        public int hashCode()
        {
            return hashCode;
        }
    }
    
    @Override
    public String toString()
    {
        return map.toString();
    }
}
