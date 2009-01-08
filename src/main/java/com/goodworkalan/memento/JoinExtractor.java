package com.goodworkalan.memento;

import java.io.Serializable;


public class JoinExtractor
implements Strata.FieldExtractor, Serializable
{
    private static final long serialVersionUID = 20070403L;

    public Comparable<?>[] getFields(Object txn, Object object)
    {
        return ((com.goodworkalan.memento.Record) object).keys;
    }
}