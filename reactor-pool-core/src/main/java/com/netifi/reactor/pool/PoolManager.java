package com.netifi.reactor.pool;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;

public interface PoolManager<T> extends Disposable {

    Publisher<T> create();

    void dispose(T pooled);

    Publisher<Void> isAlive(T pooled);

    default void dispose() {
    }
}
