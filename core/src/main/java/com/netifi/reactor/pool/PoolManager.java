package com.netifi.reactor.pool;

import org.reactivestreams.Publisher;

public interface PoolManager<T> {

    Publisher<T> create();

    void dispose(T pooled);

    Publisher<Void> isAlive(T pooled);
}
