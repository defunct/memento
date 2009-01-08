package com.agtrz.depot;

final class Equals
implements Criteria
{
    private final Object expected;
    
    public Equals(Object object)
    {
        this.expected = object;
    }

    public boolean met(Object object)
    {
        return expected.equals(object);
    }
}