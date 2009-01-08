package com.agtrz.depot;

import java.io.OutputStream;

public interface Marshaller
{
    public void marshall(OutputStream out, Object object);
}