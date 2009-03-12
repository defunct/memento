package com.goodworkalan.memento;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.goodworkalan.pack.Mutator;

public class JoinTable implements Iterable<Join>
{
    private final Map<Link, Join> table = new HashMap<Link, Join>();
    
    private final PackFactory storage;

    private final Snapshot snapshot;
    
    private final Mutator mutator;
    
    private final JoinSchemaTable joinSchemas;
    
    private final List<Janitor> janitors = new ArrayList<Janitor>();
    
    public JoinTable(PackFactory storage, Snapshot snapshot, Mutator mutator, JoinSchemaTable joinSchemas)
    {
        this.storage = storage;
        this.snapshot = snapshot;
        this.mutator = mutator;
        this.joinSchemas = joinSchemas;
    }

    public Join get(Link link)
    {
        Join join = table.get(link);
        if (join == null)
        {
            JoinSchema joinSchema = joinSchemas.get(link);
            join = new Join(storage, snapshot, mutator, joinSchema, janitors);
        }
        return join;
    }

    public Iterator<Join> iterator()
    {
        return table.values().iterator();
    }
}
