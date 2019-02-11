package com.netifi.reactor.pool;

import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class NonBlockingPool<T> extends AtomicBoolean implements Pool<T> {
    private static final Logger logger = LoggerFactory.getLogger(NonBlockingPool.class);
    private static final Duration POOLED_VALIDATION_INTERVAL = Duration.ofSeconds(30);
    private static final Duration POOLED_TIMEOUT_INTERVAL = Duration.ofSeconds(5);
    private static final int MAX_PENDING_REQUESTS_FACTOR = 3;
    private static final Exception CLOSED = new PoolClosedException();
    private static final Mono<?> CLOSED_MONO = Mono.error(CLOSED);

    private static final AtomicIntegerFieldUpdater<NonBlockingPool> WIP =
            AtomicIntegerFieldUpdater.newUpdater(NonBlockingPool.class, "wip");
    private volatile int wip;
    final Queue<InnerMember> members;
    private final Queue<MonoSink<Mono<InnerMember>>> sinks;
    private final long pooledValidationInterval;
    private final PoolManager<T> poolManager;
    private final int maxPendingRequestsCount;
    private final Duration pooledTimeout;

    public NonBlockingPool(int poolSize,
                           int maxPendingRequestsCount,
                           Duration pooledValidationInterval,
                           Duration pooledTimeout,
                           PoolManager<T> poolManager) {
        Utils.requirePositive(poolSize);
        this.pooledTimeout = Objects.requireNonNull(pooledTimeout);
        this.maxPendingRequestsCount = Utils.requirePositive(maxPendingRequestsCount);
        this.pooledValidationInterval = Objects
                .requireNonNull(pooledValidationInterval)
                .toMillis();
        this.poolManager = Objects.requireNonNull(poolManager);
        this.members = new MpscArrayQueue<>(poolSize);
        this.sinks = new MpscArrayQueue<>(maxPendingRequestsCount);
        for (int i = 0; i < poolSize; i++) {
            InnerMember member = new InnerMember();
            members.offer(member);
        }
    }

    public NonBlockingPool(int size,
                           PoolManager<T> poolManager) {
        this(size,
                size * MAX_PENDING_REQUESTS_FACTOR,
                POOLED_VALIDATION_INTERVAL,
                POOLED_TIMEOUT_INTERVAL,
                poolManager);
    }

    @Override
    public Mono<Member<T>> member() {
        if (get()) {
            return closedMono();
        }
        return Mono.<Mono<InnerMember>>create(
                sink -> {
                    if (sinks.offer(sink)) {
                        drain();
                    } else {
                        sink.error(
                                new PoolLimitException(
                                        maxPendingRequestsCount));
                    }
                })
                .flatMap(Function.identity());
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
                    while (!sinks.isEmpty()) {
                        MonoSink<Mono<InnerMember>> sink = sinks.poll();
                        sink.error(CLOSED);
                    }
                    while (!members.isEmpty()) {
                        InnerMember member = members.poll();
                        member.disposeValue();
                    }
                    poolManager.dispose();

                } else {
                    while (!sinks.isEmpty() && !members.isEmpty()) {
                        MonoSink<Mono<InnerMember>> sink = sinks.poll();
                        InnerMember member = members.poll();
                        if (logger.isDebugEnabled()) {
                            int size = members.size();
                            logger.debug("Checking out member. Current size: {}", size);
                        }
                        sink.success(member.checkout());
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
    private static <T> Mono<T> closedMono() {
        return (Mono<T>) CLOSED_MONO;
    }

    private enum MemberState {
        UNCONNECTED,
        VALIDATING,
        CONNECTED,
        CONNECTING,
        CLOSED
    }

    private class InnerMember extends AtomicBoolean implements Member<T> {

        private final AtomicBoolean checkedOut = new AtomicBoolean();

        private volatile T pooled;

        private volatile long lastPooledValidationMillis;

        private volatile Mono<InnerMember> connectionMono;

        private volatile Mono<InnerMember> validatingMono;

        private final AtomicReference<MemberState> state =
                new AtomicReference<>(MemberState.UNCONNECTED);

        private Mono<InnerMember> checkout() {
            checkedOut.set(true);
            switch (state.get()) {
                case UNCONNECTED:
                    return connect();
                case CONNECTED:
                    return tryValidate();
                case CONNECTING:
                    return connectionMono;
                case VALIDATING:
                    return validatingMono;
                case CLOSED:
                default:
                    return closedMono();
            }
        }

        private Mono<InnerMember> connect() {
            state.set(MemberState.CONNECTING);
            connectionMono =
                    Mono.from(poolManager
                            .create())
                            .timeout(pooledTimeout)
                            .doOnSuccess(pooled -> {
                                this.lastPooledValidationMillis = now();
                                this.pooled = pooled;
                                if (!state.compareAndSet(
                                        MemberState.CONNECTING,
                                        MemberState.CONNECTED)) {
                                    checkin();
                                }
                            })
                            .doOnError(throwable -> {
                                logger.error("error obtaining pooled item", throwable);
                                state.compareAndSet(
                                        MemberState.CONNECTING,
                                        MemberState.UNCONNECTED);
                                checkin();
                            })
                            .doOnCancel(() -> {
                                state.compareAndSet(
                                        MemberState.CONNECTING,
                                        MemberState.UNCONNECTED);
                                checkin();
                            })
                            .thenReturn(this)
                            .cache();

            return connectionMono;
        }

        private Mono<InnerMember> tryValidate() {
            if (now() - lastPooledValidationMillis > pooledValidationInterval) {
                state.set(MemberState.VALIDATING);
                validatingMono =
                        Mono.from(poolManager.isAlive(pooled))
                                .timeout(pooledTimeout)
                                .doOnSuccess(
                                        v -> {
                                            lastPooledValidationMillis =
                                                    System.currentTimeMillis();
                                            state.compareAndSet(
                                                    MemberState.VALIDATING,
                                                    MemberState.CONNECTED);
                                        })
                                .doOnError(
                                        throwable -> {
                                            logger.error("error validating pooled item", throwable);
                                            state.compareAndSet(
                                                    MemberState.VALIDATING,
                                                    MemberState.UNCONNECTED);
                                            checkin();
                                        })
                                .doOnCancel(
                                        () -> {
                                            state.compareAndSet(
                                                    MemberState.VALIDATING,
                                                    MemberState.CONNECTED);
                                            checkin();
                                        })
                                .thenReturn(this)
                                .cache();

                return validatingMono;
            } else {
                return connectionMono;
            }
        }

        @Override
        public T value() {
            return pooled;
        }

        public void disposeValue() {
            if (compareAndSet(false, true)) {
                state.set(MemberState.CLOSED);
                if (pooled != null) {
                    poolManager.dispose(pooled);
                    pooled = null;
                }
            }
        }

        @Override
        public void checkin() {
            if (checkedOut.compareAndSet(true, false)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Checking in member. Current size: {}", members.size());
                }
                members.offer(this);
                drain();
            } else {
                throw new IllegalStateException("Duplicate checkin is not allowed");
            }
        }
    }

    private static long now() {
        return System.currentTimeMillis();
    }
}
