package com.agtrz.depot;

import java.io.Serializable;

public class JoinExtractor
implements Strata.FieldExtractor, Serializable
{
    private static final long serialVersionUID = 20070403L;

    public Comparable<?>[] getFields(Object txn, Object object)
    {
        return ((Join.Record) object).keys;
    }
}