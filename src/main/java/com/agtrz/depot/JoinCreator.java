package com.agtrz.depot;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JoinCreator
{
    private final Set<String> setOfBinNames;

    final Map<String, String> mapOfFields;

    final List<String[]> listOfAlternates;

    public Creator(Set<String> setOfBinNames)
    {
        this.setOfBinNames = setOfBinNames;
        this.mapOfFields = new LinkedHashMap<String, String>();
        this.listOfAlternates = new ArrayList<String[]>();
    }

    public Join.Creator add(String fieldName, String binName)
    {
        if (!setOfBinNames.contains(binName))
        {
            throw new IllegalStateException();
        }
        if (mapOfFields.containsKey(fieldName))
        {
            throw new IllegalStateException();
        }
        mapOfFields.put(fieldName, binName);
        return this;
    }

    public Join.Creator add(String binName)
    {
        return add(binName, binName);
    }

    public void alternate(String[] fields)
    {
        Set<String> seen = new HashSet<String>();
        if (fields.length > mapOfFields.size())
        {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < fields.length; i++)
        {
            if (seen.contains(fields[i]))
            {
                throw new IllegalArgumentException();
            }
            if (!mapOfFields.containsKey(fields[i]))
            {
                throw new IllegalArgumentException();
            }
            seen.add(fields[i]);
        }
        listOfAlternates.add(fields);
    }
}