package com.netifi.reactor.pool;

import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class PartitionedThreadPool<T> extends AtomicBoolean implements Pool<T> {
    /*Limit number of threads using this pool instance. Should be configured with
     * system property*/
    private static final int POOL_THREADS_LIMIT = 100;
    private static final Mono<?> POOL_LIMIT_MONO = Mono.error(new PoolLimitException(POOL_THREADS_LIMIT));
    private static final Mono<?> POOL_CLOSED_MONO = Mono.error(new PoolClosedException());
    private static final Logger logger = LoggerFactory.getLogger(PartitionedThreadPool.class);

    private final ThreadLocal<Pool<T>> innerPools = new ThreadLocal<>();
    private final Queue<Pool<T>> pools = new MpscArrayQueue<>(POOL_THREADS_LIMIT);
    private final PoolFactory<T> poolFactory;

    private static final AtomicIntegerFieldUpdater<PartitionedThreadPool> WIP =
            AtomicIntegerFieldUpdater.newUpdater(PartitionedThreadPool.class, "wip");
    private volatile int wip;

    public PartitionedThreadPool(PoolFactory<T> poolFactory) {
        this.poolFactory = Objects.requireNonNull(poolFactory);
    }

    @Override
    public Mono<Member<T>> member() {
        if (get()) {
            return poolClosedMono();
        } else {
            Pool<T> pool = getPool();
            return pool == null
                    ? poolLimitMono()
                    : pool.member();
        }
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
                        close(pools.poll());
                    }
                }

                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }

    /*Returns thread local pool, or null if client thread count
     * is exceeded*/
    private Pool<T> getPool() {
        Pool<T> pool = innerPools.get();
        if (pool == null) {
            pool = poolFactory.create();
            if (pools.offer(pool)) {
                logger.info("Create pool for thread: {}", Thread.currentThread());
                innerPools.set(pool);
                drain();
            } else {
                close(pool);
                return null;
            }
        }
        return pool;
    }

    private static <T> Mono<T> poolLimitMono() {
        return (Mono<T>) POOL_LIMIT_MONO;
    }

    private static <T> Mono<T> poolClosedMono() {
        return (Mono<T>) POOL_CLOSED_MONO;
    }

    private static <T> void close(Pool<T> pool) {
        try {
            pool.close();
        } catch (Exception e) {
            logger.error("Error closing pool", e);
        }
    }
}
