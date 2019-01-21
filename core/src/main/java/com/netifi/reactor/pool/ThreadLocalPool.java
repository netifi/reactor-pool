package com.netifi.reactor.pool;

import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import io.netty.util.internal.shaded.org.jctools.util.Pow2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

public class ThreadLocalPool<T> extends AtomicBoolean implements Pool<T> {
    private static final Logger logger = LoggerFactory.getLogger(ThreadLocalPool.class);
    private static final int SINKS_SIZE = 2000;
    private static final Throwable CLOSED = new PoolClosedException();
    private static final ThreadLocal<
            Map<ThreadLocalPool,
                    Pool>> threadLocalPools =
            ThreadLocal.withInitial(HashMap::new);
    final Queue<Pool<T>> pools = new ConcurrentLinkedQueue<>();
    final Queue<MonoSink<Mono<Member<T>>>> sinks = new MpscArrayQueue<>(SINKS_SIZE);

    private final PoolFactory<T> poolFactory;
    private static final AtomicIntegerFieldUpdater<ThreadLocalPool> WIP =
            AtomicIntegerFieldUpdater.newUpdater(ThreadLocalPool.class, "wip");
    private volatile int wip;

    public ThreadLocalPool(PoolFactory<T> poolFactory) {
        this.poolFactory = poolFactory;
    }

    @Override
    public Mono<Member<T>> member() {
        return Mono.<Mono<Member<T>>>create(s -> {
            sinks.offer(s);
            drain();
        }).flatMap(Function.identity());
    }

    @Override
    public void close() {
        if (compareAndSet(false, true)) {
            drain();
        }
    }

    private void drain() {
        if (wip == 0 && WIP.getAndIncrement(this) == 0) {
            int missed = 1;
            for (; ; ) {
                if (get()) {
                    while (!pools.isEmpty()) {
                        Pool<T> pool = pools.poll();
                        try {
                            pool.close();
                        } catch (Exception e) {
                            logger.error("Error closing pool", e);
                        }
                    }
                    while (!sinks.isEmpty()) {
                        MonoSink<Mono<Member<T>>> sink = sinks.poll();
                        sink.error(CLOSED);
                    }
                } else {
                    while (!sinks.isEmpty()) {
                        MonoSink<Mono<Member<T>>> sink = sinks.poll();
                        sink.success(getPool().member());
                    }
                }

                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
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
