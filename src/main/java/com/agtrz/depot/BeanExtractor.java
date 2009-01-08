package com.agtrz.depot;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;

import com.goodworkalan.memento.Danger;
import com.goodworkalan.memento.Error;


public final class BeanExtractor
implements FieldExtractor
{
    private static final long serialVersionUID = 20070917L;

    private final Class<? extends Object> type;

    private final String[] methods;

    public BeanExtractor(Class<? extends Object> type, String[] fields)
    {
        BeanInfo beanInfo;
        try
        {
            beanInfo = Introspector.getBeanInfo(type);
        }
        catch (IntrospectionException e)
        {
            throw new Danger("Extractor", e, 1);
        }
        String[] methods = new String[fields.length];
        PropertyDescriptor[] properties = beanInfo.getPropertyDescriptors();
        for (int i = 0; i < properties.length; i++)
        {
            for (int j = 0; j < fields.length; j++)
            {
                if (properties[i].getName().equals(fields[j]))
                {
                    methods[j] = properties[i].getReadMethod().getName();
                }
            }
        }
        for (int i = 0; i < methods.length; i++)
        {
            if (methods[i] == null)
            {
                throw new Error("A", 1);
            }
        }
        this.type = type;
        this.methods = methods;
    }

    public Comparable<?>[] getFields(Object object)
    {
        Comparable<?>[] comparables = new Comparable[methods.length];
        for (int i = 0; i < methods.length; i++)
        {
            try
            {
                comparables[i] = (Comparable<?>) type.getMethod(methods[0], new Class[0]).invoke(object, new Object[0]);
            }
            catch (Exception e)
            {
                throw new Danger("A.", e, 0);
            }
        }
        return comparables;
    }
}