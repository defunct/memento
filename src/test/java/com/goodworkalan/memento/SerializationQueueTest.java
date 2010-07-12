package com.goodworkalan.memento;

import java.util.Collections;

import org.testng.annotations.Test;

/**
 * Unit tests for the {@link SerializationQueue} class.
 *
 * @author Alan Gutierrez
 */
public class SerializationQueueTest {
    /** Test interrupting the queue. */
    @Test
    public void interrupted() throws InterruptedException {
        SerializationQueue queue = new SerializationQueue();
        Thread thread = new Thread(queue);
        thread.start();
        thread.interrupt();
        queue.queue.offer(Collections.EMPTY_MAP);
        thread.join();
    }
}
