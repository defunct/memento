package com.goodworkalan.memento;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * React to requests to write types to the data store. 
 *
 * @author Alan Gutierrez
 */
public class Serializer {
    /** 
    /** The array of serialization queues. */
    private final SerializationQueue[] queues;
    
    /** The array of serialization queue threads. */
    private final Thread[] threads;

    /**
     * Create a new serializer with the given number of queues.
     * 
     * @param configuration
     *            The serializer configuraiton.
     */
    public Serializer(SerializerConfiguration configuration) { 
        SerializationQueue[] queues = new SerializationQueue[configuration.queues];
        for (int i = 0; i < configuration.queues; i++) {
            queues[i] = new SerializationQueue();
        }
        this.queues = queues;
        this.threads = new Thread[configuration.queues];
    }

    /**
     * Start the serialization queues.
     * 
     * @exception IllegalThreadStateException
     *                If the queues are already running or if the queues have
     *                terminated.
     */
    public synchronized void start() {
        if (threads[0] != null) {
            throw new IllegalThreadStateException();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(queues[i]);
            threads[i].start();
        }
    }

    /**
     * Ordered shutdown of all of the queues. The method blocks until all of the
     * threads have shutdown.
     */
    public synchronized void stop() {
        if (threads[0] == null) {
            throw new IllegalThreadStateException();
        }
        for (int i = 0; i < queues.length; i++) {
            queues[i].queue.offer(Collections.EMPTY_MAP);
        }
        for (int i = 0; i < queues.length; i++) {
            final Thread thread = threads[i];
            retry(new Callable<Object>() {
                public Object call() throws InterruptedException {
                    thread.join();
                    return null;
                }
            });
            threads[i] = null;
        }
    }

    /**
     * Read the object and return UUID.
     * 
     * @param uuid
     *            The object identifier.
     * @return The object off of the wire.
     */
    public ByteBuffer read(UUID uuid) {
        
        return null;
    }

    /**
     * Get the future value, trying again if an
     * <code>InterruptedException</code> is thrown.
     * 
     * @param <T>
     *            The type of future value.
     * @param future
     *            The future.
     * @return The future value.
     * @throws ExecutionException
     *             If an exception is thrown during the execution of the future
     *             task.
     */
    static <T> T retry(FutureTask<T> future) throws ExecutionException {
        for (;;) {
            try {
                return future.get();
            } catch (InterruptedException e) {
                continue;
            }
        }
    }

    /**
     * Invoke the callable, trying again if an interrupted exception is thrown.
     * 
     * @param <T>
     *            The type of return value.
     * @param callable
     *            The procedure.
     * @return The return value.
     */
    static <T> T retry(Callable<T> callable) {
        for (;;) {
            try {
                return callable.call();
            } catch (InterruptedException e) {
                continue;
            } catch (Exception e) {
                // We will only call this method with callables that throw an
                // interrupted exception.
                throw new RuntimeException(e);
            }
        }
    }
}
