package com.agtrz.depot;

import java.io.Serializable;
import java.util.Map;

import com.agtrz.depot.Join.Schema;

public class JoinSchema
implements Serializable
{
    private static final long serialVersionUID = 20070208L;

    public final Map<String, String> mapOfFields;

    public final Join.Index[] indices;

    public Schema(Join.Index[] indices, Map<String, String> mapOfFields)
    {
        this.indices = indices;
        this.mapOfFields = mapOfFields;
    }

    public Join.Schema toStrata(Object txn)
    {
        Join.Index[] indexes = new Join.Index[this.indices.length];
        for (int i = 0; i < indexes.length; i++)
        {
            indexes[i] = this.indices[i].toStrata(txn);
        }
        return new Schema(indexes, mapOfFields);
    }

    public Join.Schema toSchema()
    {
        Join.Index[] indexes = new Join.Index[this.indices.length];
        for (int i = 0; i < indexes.length; i++)
        {
            indexes[i] = this.indices[i].toSchema();
        }
        return new Schema(indexes, mapOfFields);
    }
}



