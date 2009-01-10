package com.goodworkalan.memento;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class IndexSchemaStash
{
    private final Map<List<Object>, Object> indexSchemas = new HashMap<List<Object>, Object>();

    public <Item, Fields> IndexSchema<Item, Fields> get(Class<Item> itemClass, Class<Fields> fieldsClass, String name)
    {
        return new TypeCaster<IndexSchema<Item, Fields>>().cast(indexSchemas.get(Arrays.asList(itemClass, fieldsClass, name)));
    }

    public <Item, Fields> void put(Class<Item> itemClass, Class<Fields> fieldsClass, String name, IndexSchema<Item, Fields> schema)
    {
        indexSchemas.put(Arrays.<Object>asList(itemClass, fieldsClass, name), schema);
    }
}
