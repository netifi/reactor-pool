package com.netifi.reactor.pool;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

class TestPoolManager implements PoolManager<TestPoolManager.Resource> {

    @Override
    public Publisher<Resource> create() {
        return Mono.just(new Resource());
    }

    @Override
    public void dispose(Resource pooled) {
        pooled.dispose();
    }

    @Override
    public Publisher<Void> isAlive(Resource pooled) {
        return Mono
                .defer(
                        () -> pooled.isDisposed()
                                ? Mono.error(new RuntimeException())
                                : Mono.empty());
    }

    public static class Resource {
        private volatile boolean disposed = false;

        public void dispose() {
            disposed = true;
        }

        public boolean isDisposed() {
            return disposed;
        }
    }
}
