package com.goodworkalan.memento;

import java.io.InputStream;

public interface Unmarshaller
{
    public Object unmarshall(InputStream in);
}