package com.netifi.reactor.pool;

import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadLocalPool<T> extends AtomicBoolean implements Pool<T> {
    private static final ThreadLocal<
            Map<ThreadLocalPool,
                    Pool>> threadLocalPools =
            ThreadLocal.withInitial(HashMap::new);
    final Queue<Pool<T>> pools = new ConcurrentLinkedQueue<>();
    private final PoolFactory<T> poolFactory;

    public ThreadLocalPool(PoolFactory<T> poolFactory) {
        this.poolFactory = poolFactory;
    }

    @Override
    public Mono<Member<T>> member() {
        return Mono.defer(() -> getPool().member());
    }

    //todo race with pools.offer in getPool
    @Override
    public void close() throws Exception {
        if (compareAndSet(false, true)) {
            while (!pools.isEmpty()) {
                Pool<T> pool = pools.poll();
                pool.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Pool<T> getPool() {
        Map<ThreadLocalPool, Pool> poolsMap = threadLocalPools.get();
        Pool<T> pool = poolsMap.get(this);
        if (pool == null) {
            pool = poolFactory.create();
            poolsMap.put(this, pool);
            this.pools.offer(pool);
        }
        return pool;
    }
}
