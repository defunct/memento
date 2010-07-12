package com.goodworkalan.memento;

import static com.goodworkalan.comfort.io.Files.unlink;

import java.io.File;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Unit tests for the {@link Store} class.
 *
 * @author Alan Gutierrez
 */
public class StoreTest {
    /** The database service. */
    private Serializer serializer;
    
    /** Startup the database service. */
    @BeforeTest
    public void start() {
        SerializerConfiguration configuration = new SerializerConfiguration();
        configuration.dataDirectory = new File(new File(".").getAbsoluteFile(), "target/database");
        unlink(configuration.dataDirectory);
        configuration.dataDirectory.mkdirs();
        serializer = new Serializer(configuration);
        serializer.start();
    }
    
    /** Stop the database service. */
    @AfterTest
    public void stop() {
        serializer.stop();
    }

    /** Test a non-repeatable read. */
    @Test
    public void nonRepeatable() {
        Store store = new Store(serializer);
        store.commit(new Mutation() {
            int count = 0;
            public void mutate(Mutator mutator) {
                if (count++ == 0) {
                    throw new NonRepatableReadException();
                }
            }
        });
    }
}
