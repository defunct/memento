package com.goodworkalan.memento;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A working queue that serializes string beans into the persistant data store.
 *
 * @author Alan Gutierrez
 */
class SerializationQueue implements Runnable {
    /** A queue of Twitter requests waiting to be fed to the reactor. */
    public final LinkedBlockingQueue<Map<?, ?>> queue = new LinkedBlockingQueue<Map<?, ?>>();
    
    /** Pull items off of the queue and serialize them. */
    public void run() {
        for (;;) {
            try {
                Map<?, ?> request = queue.take();
                if (request.isEmpty()) {
                    break;
                }
            } catch (InterruptedException e) {
                continue;
            }
        }
    }
}
