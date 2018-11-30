package reactor.pool;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

public final class NonBlockingPool<T> implements Pool<T> {

    final Mono<? extends T> factory;
    final Predicate<? super T> healthCheck;
    final long idleTimeBeforeHealthCheckMs;
    final Consumer<? super T> disposer;
    final int maxSize;
    final long maxIdleTimeMs;
    final long createRetryIntervalMs;
    final BiFunction<? super T, ? super Checkin, ? extends T> checkinDecorator;
    final Scheduler scheduler;
    final Runnable closeAction;

    private final AtomicReference<MonoMember<T>> member = new AtomicReference<>();
    private volatile boolean closed;

    NonBlockingPool(Mono<? extends T> factory, Predicate<? super T> healthCheck, Consumer<? super T> disposer,
            int maxSize, long idleTimeBeforeHealthCheckMs, long maxIdleTimeMs, long createRetryIntervalMs,
            BiFunction<? super T, ? super Checkin, ? extends T> checkinDecorator, Scheduler scheduler,
            Runnable closeAction) {
        Objects.requireNonNull(factory);
        Objects.requireNonNull(healthCheck);
        Objects.requireNonNull(disposer);
        if (maxSize <= 0) {
            throw new IllegalArgumentException();
        }
        Objects.requireNonNull(checkinDecorator);
        Objects.requireNonNull(scheduler);
        if (createRetryIntervalMs < 0) {
            throw new IllegalArgumentException("createRetryIntervalMs must be >=0");
        }
        Objects.requireNonNull(closeAction);
        if (maxIdleTimeMs < 0) {
            throw new IllegalArgumentException("maxIdleTime must be >=0");
        }
        this.factory = factory;
        this.healthCheck = healthCheck;
        this.disposer = disposer;
        this.maxSize = maxSize;
        this.idleTimeBeforeHealthCheckMs = idleTimeBeforeHealthCheckMs;
        this.maxIdleTimeMs = maxIdleTimeMs;
        this.createRetryIntervalMs = createRetryIntervalMs;
        this.checkinDecorator = checkinDecorator;
        this.scheduler = scheduler;// schedules retries
        this.closeAction = closeAction;
    }

    private MonoMember<T> createMember() {
        return new MonoMember<T>(this, Queues.unboundedMultiproducer());
    }

    @Override
    public Mono<Member<T>> member() {
        while (true) {
            MonoMember<T> m = member.get();
            if (m != null)
                return m;
            else {
                m = createMember();
                if (member.compareAndSet(null, m)) {
                    return m;
                }
            }
        }
    }

    public void checkin(Member<T> m) {
        MonoMember<T> mem = member.get();
        if (mem != null) {
            mem.checkin(m);
        }
    }

    @Override
    public void close() {
        closed = true;
        while (true) {
            MonoMember<T> m = member.get();
            if (m == null) {
                return;
            } else if (member.compareAndSet(m, null)) {
                m.close();
                break;
            }
        }

        closeAction.run();
    }

    boolean isClosed() {
        return closed;
    }

    public static <T> Builder<T> factory(Mono<? extends T> factory) {
        return new Builder<T>().factory(factory);
    }

    public static class Builder<T> {

        private static final Predicate<Object> ALWAYS_TRUE = new Predicate<Object>() {
            @Override
            public boolean test(Object o) {
                return true;
            }
        };

        private static final BiFunction<Object, Checkin, Object> DEFAULT_CHECKIN_DECORATOR = (x, y) -> x;
        private Mono<? extends T> factory;
        private Predicate<? super T> healthCheck = ALWAYS_TRUE;
        private long idleTimeBeforeHealthCheckMs = 1000;
        private Consumer<? super T> disposer = Consumers.doNothing();
        private int maxSize = 10;
        private long createRetryIntervalMs = 30000;
        private Scheduler scheduler = Schedulers.parallel();
        private long maxIdleTimeMs;
        @SuppressWarnings("unchecked")
        private BiFunction<? super T, ? super Checkin, ? extends T> checkinDecorator = (BiFunction<T, Checkin, T>) DEFAULT_CHECKIN_DECORATOR;
        private Runnable closeAction = () -> {
        };

        private Builder() {
        }

        public Builder<T> factory(Mono<? extends T> factory) {
            Objects.requireNonNull(factory);
            this.factory = factory;
            return this;
        }

        public Builder<T> healthCheck(Predicate<? super T> healthCheck) {
            Objects.requireNonNull(healthCheck);
            this.healthCheck = healthCheck;
            return this;
        }

        public Builder<T> idleTimeBeforeHealthCheck(long duration, TimeUnit unit) {
            if (duration < 0) {
                throw new IllegalArgumentException();
            }
            this.idleTimeBeforeHealthCheckMs = unit.toMillis(duration);
            return this;
        }
        
        /**
         * Sets the maximum time a connection can remaing idle before being scheduled
         * for release (closure). If set to 0 (regardless of unit) then idle connections
         * are not released. If not set the default value is 30 minutes.
         * 
         * @param value duration
         * @param unit  unit of the duration
         * @return this
         */
        public Builder<T> maxIdleTime(long value, TimeUnit unit) {
            this.maxIdleTimeMs = unit.toMillis(value);
            return this;
        }

        public Builder<T> createRetryInterval(long duration, TimeUnit unit) {
            this.createRetryIntervalMs = unit.toMillis(duration);
            return this;
        }

        public Builder<T> disposer(Consumer<? super T> disposer) {
            Objects.requireNonNull(disposer);
            this.disposer = disposer;
            return this;
        }

        public Builder<T> maxSize(int maxSize) {
            if (maxSize <= 0) {
                throw new IllegalArgumentException();
            }
            this.maxSize = maxSize;
            return this;
        }

        public Builder<T> scheduler(Scheduler scheduler) {
            Objects.requireNonNull(scheduler);
            this.scheduler = scheduler;
            return this;
        }

        public Builder<T> checkinDecorator(BiFunction<? super T, ? super Checkin, ? extends T> f) {
            this.checkinDecorator = f;
            return this;
        }

        public Builder<T> onClose(Runnable closeAction) {
            this.closeAction = closeAction;
            return this;
        }

        public NonBlockingPool<T> build() {
            return new NonBlockingPool<T>(factory, healthCheck, disposer, maxSize, idleTimeBeforeHealthCheckMs,
                    maxIdleTimeMs, createRetryIntervalMs, checkinDecorator, scheduler, closeAction);
        }

    }

}
