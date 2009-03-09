package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.ilk.Ilk;

public class JoinSchemaTable
{
    private final Map<Link, JoinSchema> table = new HashMap<Link, JoinSchema>();
    
    public JoinSchema get(Link link)
    {
        JoinSchema joinSchema = table.get(link);
        if (joinSchema == null)
        {
            joinSchema = new JoinSchema(link);
            table.put(link, joinSchema);
        }
        return joinSchema;
    }
    
    public boolean has(Ilk<?> ilk)
    {
        return table.containsKey(ilk);
    }
}
