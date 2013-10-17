/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.util;

import org.pickle.Disposable;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 */
public abstract class PersistentBlockingQueueConsumer<E extends Serializable>
        implements Runnable, Disposable {

    private static final Logger log = Logger.getLogger(PersistentBlockingQueueConsumer.class);
    private PersistentBlockingQueue<E> queue;
    private boolean disposed;
    private long interval;

    PersistentBlockingQueueConsumer(PersistentBlockingQueue<E> queue) {
        this(queue, 250L);
    }

    PersistentBlockingQueueConsumer(PersistentBlockingQueue<E> queue, long pollingInterval) {
        this.queue = queue;
        this.interval = pollingInterval;
    }

    @Override
    public final void dispose() {
        disposed = true;
    }

    @Override
    public final void run() {
        while (!disposed) {
            try {
                final E object = queue.poll(interval, TimeUnit.MILLISECONDS);
                if (object != null) {
                    queue.inTransaction(new Runnable() {
                        public void run() {
                            objectTaken(object);
                        }
                    });
                }
            } catch (InterruptedException e) {
                log.log(Level.DEBUG, "Consumer caught InterruptedException.  Continuing to consume anyway.", e);
            }
        }
    }

    /**
     * Called by the run() loop whenever a new object is taken from the queue.
     * The subclass implementation should process the object accordingly. If an
     * error is encountered which should cause the object to remain on the
     * queue, then this method should throw a RuntimeException of any kind.
     *
     * @param object the object taken from the queue
     * @throws RuntimeException if processing failed and the object should
     * remain on the queue for a retry
     */
    protected abstract void objectTaken(E object);
}
