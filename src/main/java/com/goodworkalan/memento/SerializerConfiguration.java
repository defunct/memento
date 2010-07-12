package com.goodworkalan.memento;

import java.io.File;

/**
 * A configuration structure for a serialzier.
 * 
 * @author Alan Gutierrez
 */
public class SerializerConfiguration {
    /** The number of queues to start. */
    public int queues = 4;
    
    /** The data directory. */
    public File dataDirectory;
}
