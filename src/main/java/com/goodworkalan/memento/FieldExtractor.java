package com.goodworkalan.memento;

import java.io.Serializable;

public interface FieldExtractor
extends Serializable
{
    public abstract Comparable<?>[] getFields(Object object);
}