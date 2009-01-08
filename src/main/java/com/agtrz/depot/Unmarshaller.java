package com.agtrz.depot;

import java.io.InputStream;

public interface Unmarshaller
{
    public Object unmarshall(InputStream in);
}