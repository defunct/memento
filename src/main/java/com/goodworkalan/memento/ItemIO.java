package com.goodworkalan.memento;

import java.io.InputStream;
import java.io.OutputStream;

public interface ItemIO<Item>
{
    public Item read(InputStream in);
    
    public void write(OutputStream out, Item item);
}
