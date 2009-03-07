package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

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
    
    public boolean has(Item<?> item)
    {
        return table.containsKey(item);
    }
}
