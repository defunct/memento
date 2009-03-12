package com.goodworkalan.memento;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
 
// TODO Document.
public class WeakIdentityLookup
{
    // TODO Document.
    private final Map<WeakKey, Long> map;
    
    // TODO Document.
    private final ReferenceQueue<Object> queue;
    
    // TODO Document.
    public WeakIdentityLookup()
    {
        this.map = new HashMap<WeakKey, Long>();
        this.queue = new ReferenceQueue<Object>();
    }
    
    // TODO Document.
    private void collect()
    {
        WeakKey key;
        while ((key = (WeakKey) queue.poll()) != null) 
        {
            map.remove(key);
        }
    }
    
    // TODO Document.
    public Long get(Object item)
    {
        collect();
        return map.get(item);
    }
    
    // TODO Document.
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
    
    // TODO Document.
    private final static class SearchKey
    {
        // TODO Document.
        public final Object referant;
        
        // TODO Document.
        public SearchKey(Object referant)
        {
            this.referant = referant;
        }
        
        // TODO Document.
        @Override
        public boolean equals(Object object)
        {
            return ((WeakKey) object).equals(this);
        }
        
        // TODO Document.
        @Override
        public int hashCode()
        {
            return referant.hashCode();
        }
    }
    
    // TODO Document.
    private final static class WeakKey extends WeakReference<Object> 
    {
        // TODO Document.
        private final int hashCode;
        
        // TODO Document.
        public WeakKey(Object referant, ReferenceQueue<Object> queue)
        {
            super(referant, queue);
            this.hashCode = referant.hashCode();
        }

        // TODO Document.
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
        
       // TODO Document.
        @Override
        public int hashCode()
        {
            return hashCode;
        }
    }
    
    // TODO Document.
    @Override
    public String toString()
    {
        return map.toString();
    }
}
