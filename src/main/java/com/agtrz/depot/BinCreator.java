package com.agtrz.depot;

import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.memento.Marshaller;
import com.goodworkalan.memento.SerializationMarshaller;
import com.goodworkalan.memento.SerializationUnmarshaller;
import com.goodworkalan.memento.Unmarshaller;

public class BinCreator
{
    private final String name;

    private final Map<String, IndexCreator> mapOfIndices;

    private Unmarshaller unmarshaller;

    private Marshaller marshaller;

    public BinCreator(String name)
    {
        this.name = name;
        this.mapOfIndices = new HashMap<String, IndexCreator>();
        this.unmarshaller = new SerializationUnmarshaller();
        this.marshaller = new SerializationMarshaller();
    }

    public String getName()
    {
        return name;
    }

    public IndexCreator newIndex(String name)
    {
        if (mapOfIndices.containsKey(name))
        {
            throw new IllegalStateException();
        }
        IndexCreator newIndex = new IndexCreator();
        mapOfIndices.put(name, newIndex);
        return newIndex;
    }

    public void setMarshallers(Unmarshaller unmarshaller, Marshaller marshaller)
    {
        this.unmarshaller = unmarshaller;
        this.marshaller = marshaller;
    }
}