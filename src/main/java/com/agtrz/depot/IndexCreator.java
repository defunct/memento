package com.agtrz.depot;

import com.goodworkalan.memento.Unmarshaller;

public class IndexCreator
{
    private Unmarshaller unmarshaller;

    private FieldExtractor extractor;

    private boolean unique;

    private boolean notNull;

    public IndexCreator()
    {
    }

    public void setExtractor(Class<? extends Object> type, String field)
    {
        setExtractor(type, new String[] { field });
    }

    public void setExtractor(Class<? extends Object> type, String[] fields)
    {
        setExtractor(new BeanExtractor(type, fields));
    }

    public void setExtractor(FieldExtractor extractor)
    {
        this.extractor = extractor;
    }

    public void setUnmarshaller(Unmarshaller unmarshaller)
    {
        this.unmarshaller = unmarshaller;
    }

    public void setUnique(boolean unique)
    {
        this.unique = unique;
    }

    public void setNotNull(boolean notNull)
    {
        this.notNull = notNull;
    }
}
