package com.goodworkalan.memento;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Tree;

final class BinSchema
implements Serializable
{
    private static final long serialVersionUID = 20070408L;

    public final DepotSchema schema;
    
    public final Tree<BinRecord, Mutator> strata;

    public final Map<String, IndexSchema> mapOfIndexSchemas;

    public BinSchema(DepotSchema schema, Tree<BinRecord, Mutator> strata, Map<String, IndexSchema> mapOfIndexSchemas)
    {
        this.schema = schema;
        this.strata = strata;
        this.mapOfIndexSchemas = mapOfIndexSchemas;
    }

    public com.goodworkalan.strata.Tree<BinRecord, Mutator> getStrata()
    {
        return strata;
    }

    private Map<String, IndexSchema> newMapOfIndexStratas(Map<String, IndexSchema> mapOfIndexSchemas, Object txn)
    {
        Map<String, IndexSchema> mapOfIndexStratas = new HashMap<String, IndexSchema>();
        for(Map.Entry<String, IndexSchema> entry : mapOfIndexSchemas.entrySet())
        {
            String name = entry.getKey();
            IndexSchema schema = mapOfIndexSchemas.get(name);
            mapOfIndexStratas.put(name, schema.toStrata(txn));
        }
        return mapOfIndexStratas;
    }

    private Map<String, IndexSchema> newMapOfIndexSchemas(Map<String, IndexSchema> mapOfIndexStratas)
    {
        Map<String, IndexSchema> mapOfIndexSchemas = new HashMap<String, IndexSchema>();
        for(Map.Entry<String, IndexSchema> entry : mapOfIndexStratas.entrySet())
        {
            String name = entry.getKey();
            IndexSchema schema = mapOfIndexStratas.get(name);
            mapOfIndexSchemas.put(name, schema.toSchema());
        }
        return mapOfIndexSchemas;
    }
}