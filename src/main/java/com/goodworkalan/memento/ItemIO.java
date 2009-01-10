package com.goodworkalan.memento;

import java.io.InputStream;
import java.io.OutputStream;

public interface ItemIO<T>
{
    public T read(InputStream in);
    
    public void write(OutputStream out, T item);
}
