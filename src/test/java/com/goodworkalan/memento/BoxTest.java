package com.goodworkalan.memento;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class BoxTest
{
    @Test
    public void constructor()
    {
        Box<String> box = new Box<String>(1, 1, "Hello, World!");
        assertEquals(box.getKey(), 1L);
        assertEquals(box.getVersion(), 1L);
        assertEquals(box.getItem(), "Hello, World!");
    }
}
