package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.Map;

// FIXME Rename Joint. Need to figure out how to create these using Tuples.
// Joint, which wouldn't build, so much, but invoke the rest of the query,
// so you would filter.
public final class Tuple
{
    private final Snapshot snapshot;

    private final String[] fields;

    private final Map<String, String> mapOfFields;

    private final Join.Record record;

    public Tuple(Snapshot snapshot, Map<String, String> mapOfFields, String[] fields, Join.Record record)
    {
        this.snapshot = snapshot;
        this.mapOfFields = mapOfFields;
        this.fields = fields;
        this.record = record;
    }

    public Bag getBag(String fieldName)
    {
        for (int i = 0; i < fields.length; i++)
        {
            if (fields[i].equals(fieldName))
            {
                String bagName = (String) mapOfFields.get(fieldName);
                return snapshot.getBin(bagName).get(record.keys[i]);
            }
        }
        throw new IllegalArgumentException();
    }

    public Bag getBag(Unmarshaller unmarshaller, String fieldName)
    {
        for (int i = 0; i < fields.length; i++)
        {
            if (fields[i].equals(fieldName))
            {
                String bagName = (String) mapOfFields.get(fieldName);
                return snapshot.getBin(bagName).get(unmarshaller, record.keys[i]);
            }
        }
        throw new IllegalArgumentException();
    }

    public Map<String, Long> getKeys()
    {
        Map<String, Long> mapOfKeys = new HashMap<String, Long>();
        for (int i = 0; i < fields.length; i++)
        {
            mapOfKeys.put(fields[i], record.keys[i]);
        }
        return mapOfKeys;
    }
}