package com.goodworkalan.memento;

import static com.goodworkalan.comfort.io.Files.unlink;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.testng.annotations.Test;

/**
 * Unit tests for the {@link Serializer} class.
 *
 * @author Alan Gutierrez
 */
public class SerializerTest {
    /** Test starting and stopping the serializer. */
    @Test
    public void startStop() {
        SerializerConfiguration configuration = new SerializerConfiguration();
        configuration.dataDirectory = new File(new File(".").getAbsoluteFile(), "target/database");
        unlink(configuration.dataDirectory);
        configuration.dataDirectory.mkdirs();
        Serializer serializer = new Serializer(configuration);
        serializer.start();
        serializer.stop();
    }

    /**
     * Test retrying a callable that throws an exception that is not an
     * <code>InterruptedException</code>.
     */
    @Test(expectedExceptions = RuntimeException.class)
    public void retryException() {
        Serializer.retry(new Callable<Object>() {
            public Object call() throws IOException {
                throw new IOException();
            }
        });
    }
    
    /** Test callable retry. */
    @Test
    public void function() throws InterruptedException {
        Thread thread = new Thread() {
            public void run() {
                String a = Serializer.retry(new Callable<String>() {
                    int count;
                    public String call() throws InterruptedException {
                        if (count == 0) {
                            count++;
                            synchronized (this) {
                                wait();
                            }
                        }
                        return "A";
                    }
                });
                assertEquals(a, "A");
            }
        };
        thread.start();
        thread.interrupt();
        thread.join();
    }
    
    /** Test future retry. */
    @Test
    public void future() throws InterruptedException {
        final FutureTask<Integer> future = new FutureTask<Integer>(new Callable<Integer>() {
            public Integer call() {
                return 1;
            }
        });
        Thread consumer = new Thread() {
            @Override
            public void run() {
                int i = 0;
                try {
                    i = Serializer.retry(future);
                } catch (ExecutionException e) {
                }
                assertEquals(i, 1);
            }
        };
        consumer.start();
        consumer.interrupt();
        Thread thread = new Thread(future);
        thread.start();
        consumer.join();
        thread.join();
    }

}
