package reactor.pool;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.*;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.retry.Retry;

import java.io.Closeable;
import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.function.BiFunction;
import java.util.function.Supplier;

final class MonoMember<T> extends Mono<Member<T>> implements Closeable {

    final AtomicReference<Subscribers<T>> subscribers;

    private static final Logger log = LoggerFactory.getLogger(MonoMember.class);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static final Subscribers EMPTY = new Subscribers(new MemberMonoSubscriber[0], new boolean[0], 0, 0, 0);

    private final Queue<DecoratingMember<T>> initializedAvailable;
    private final Queue<DecoratingMember<T>> notInitialized;
    private final Queue<DecoratingMember<T>> toBeReleased;
    private final Queue<DecoratingMember<T>> toBeChecked;

    private final AtomicInteger wip = new AtomicInteger();
    private final DecoratingMember<T>[] members;
    private final Scheduler scheduler;
    private final long createRetryIntervalMs;

    // synchronized by `wip`
    private final Disposable.Composite scheduled = Disposables.composite();

    final NonBlockingPool<T> pool;

    // represents the number of outstanding member requests.
    // the number is decremented when a new member value is
    // initialized (a scheduled action with a subsequent drain call)
    // or an existing value is available from the pool (queue) (and is then
    // emitted).
    // private final AtomicLong requested = new AtomicLong();

    private final AtomicLong initializeScheduled = new AtomicLong();

    // mutable
    private volatile boolean cancelled;

    @SuppressWarnings("unchecked")
    MonoMember(NonBlockingPool<T> pool, Supplier<Queue<DecoratingMember<T>>> queueSupplier) {
        Objects.requireNonNull(pool);
        this.initializedAvailable = queueSupplier.get();
        this.notInitialized = queueSupplier.get();
        this.toBeReleased = queueSupplier.get();
        this.toBeChecked = queueSupplier.get();
        this.members = createMembersArray(pool.maxSize, pool.checkinDecorator);
        for (DecoratingMember<T> m : members) {
            notInitialized.offer(m);
        }
        this.scheduler = pool.scheduler;
        this.createRetryIntervalMs = pool.createRetryIntervalMs;
        this.subscribers = new AtomicReference<>(EMPTY);
        this.pool = pool;
    }

    private DecoratingMember<T>[] createMembersArray(int poolMaxSize,
            BiFunction<? super T, ? super Checkin, ? extends T> checkinDecorator) {
        @SuppressWarnings("unchecked")
        DecoratingMember<T>[] m = new DecoratingMember[poolMaxSize];
        for (int i = 0; i < m.length; i++) {
            m[i] = new DecoratingMember<T>(null, checkinDecorator, this);
        }
        return m;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Member<T>> actual) {
        // the action of checking out a member from the pool is implemented as a
        // subscription to the singleton MonoMember
        MemberMonoSubscriber<T> m = new MemberMonoSubscriber<T>(actual, this);

        actual.onSubscribe(m);

        if (pool.isClosed()) {
            actual.onError(new PoolClosedException());
            return;
        }
        add(m);
        if (m.isDisposed()) {
            remove(m);
        } else {
            // atomically change requested
            while (true) {
                Subscribers<T> a = subscribers.get();
                if (subscribers.compareAndSet(a, a.withRequested(a.requested + 1))) {
                    break;
                }
            }
        }
        log.debug("subscribed");
        drain();
    }

    public void checkin(Member<T> member) {
        checkin(member, false);
    }

    public void checkin(Member<T> member, boolean decrementInitializeScheduled) {
        log.debug("checking in {}", member);
        DecoratingMember<T> d = ((DecoratingMember<T>) member);
        d.scheduleRelease();
        d.markAsChecked();
        initializedAvailable.offer((DecoratingMember<T>) member);
        if (decrementInitializeScheduled) {
            initializeScheduled.decrementAndGet();
        }
        drain();
    }

    public void addToBeReleased(DecoratingMember<T> member) {
        toBeReleased.offer(member);
        drain();
    }

    public void cancel() {
        log.debug("cancel called");
        this.cancelled = true;
        disposeAll();
    }

    private void drain() {
        log.debug("drain called");
        if (wip.getAndIncrement() == 0) {
            log.debug("drain loop starting");
            int missed = 1;
            while (true) {
                // we schedule release of members even if no requests exist
                scheduleReleases();
                scheduleChecks();

                Subscribers<T> obs = subscribers.get();
                log.debug("requested={}", obs.requested);
                // max we can emit is the number of active (available) resources in pool
                long r = Math.min(obs.activeCount, obs.requested);
                long e = 0; // emitted
                // record number of attempted emits in case all subscribers have been cancelled
                // while are in the loop and we want to break
                long attempts = 0;
                while (e != r && attempts != obs.activeCount) {
                    if (cancelled) {
                        disposeAll();
                        return;
                    }
                    // check for an already initialized available member
                    final DecoratingMember<T> m = initializedAvailable.poll();
                    log.debug("poll of available members returns {}", m);
                    if (m == null) {
                        // no members available, check for a released member (that needs to be
                        // reinitialized before use)
                        final DecoratingMember<T> m2 = notInitialized.poll();
                        if (m2 == null) {
                            break;
                        } else {
                            // only schedule member initialization if there is enough demand,
                            boolean used = trySchedulingInitialization(r, e, m2);
                            if (!used) {
                                break;
                            }
                        }
                    } else if (!m.isReleasing() && !m.isChecking()) {
                        log.debug("trying to emit member");
                        if (shouldPerformHealthCheck(m)) {
                            log.debug("queueing member for health check {}", m);
                            toBeChecked.offer(m);
                        } else {
                            log.debug("no health check required for {}", m);
                            // this should not block because it just schedules emissions to subscribers
                            if (tryEmit(obs, m)) {
                                e++;
                            } else {
                                log.debug("no active subscribers");
                            }
                            attempts++;
                        }
                    }
                    // schedule release immediately of any member
                    // queued for releasing
                    scheduleReleases();
                    // schedule check of any member queued for checking
                    scheduleChecks();
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
    }

    private boolean trySchedulingInitialization(long r, long e, final DecoratingMember<T> m) {
        // check initializeScheduled using a CAS loop
        while (true) {
            long cs = initializeScheduled.get();
            if (e + cs < r) {
                if (initializeScheduled.compareAndSet(cs, cs + 1)) {
                    if (!cancelled) {
                        log.debug("scheduling member creation");
                        scheduled.add(
                            pool.factory
                                .retryWhen(
                                    Retry.onlyIf(context -> !cancelled)
                                        .fixedBackoff(Duration.ofMillis(createRetryIntervalMs))
                                        .withBackoffScheduler(scheduler))
                                .subscribe(
                                    value -> {
                                        m.setValueAndClearReleasingFlag(value);
                                        checkin(m, true);
                                    }));
                    }

                    return true;
                }
            } else {
                log.debug("insufficient demand to initialize {}", m);
                // don't need to initialize more so put back on queue and exit the loop
                notInitialized.offer(m);
                return false;
            }
        }
    }

    private boolean shouldPerformHealthCheck(final DecoratingMember<T> m) {
        long now = scheduler.now(TimeUnit.MILLISECONDS);
        log.debug("schedule.now={}, lastCheck={}", now, m.lastCheckTime());
        return pool.idleTimeBeforeHealthCheckMs > 0 && now - m.lastCheckTime() >= pool.idleTimeBeforeHealthCheckMs;
    }

    private void scheduleChecks() {
        DecoratingMember<T> m;
        while ((m = toBeChecked.poll()) != null) {
            if (!m.isReleasing()) {
                log.debug("scheduling check of {}", m);
                // we mark as checking so that we can ignore it if already in the
                // initializedAvailable queue after concurrent checkin
                m.markAsChecking();
                scheduled.add(scheduler.schedule(new Checker(m)));
            }
        }
    }

    private void scheduleReleases() {
        DecoratingMember<T> m;
        while ((m = toBeReleased.poll()) != null) {
            log.debug("scheduling release of {}", m);
            // we mark as releasing so that we can ignore it if already in the
            // initializedAvailable queue after concurrent checkin
            m.markAsReleasing();
            scheduled.add(scheduler.schedule(new Releaser(m)));
        }
    }

    private boolean tryEmit(Subscribers<T> obs, DecoratingMember<T> m) {
        // get a fresh worker each time so we jump threads to
        // break the stack-trace (a long-enough chain of
        // checkout-checkins could otherwise provoke stack
        // overflow)

        // advance counter so the next and choose a Subscriber to emit to (round robin)

        int index = obs.index;
        MemberMonoSubscriber<T> o = obs.subscribers[index];
        MemberMonoSubscriber<T> oNext = o;
        // atomically bump up the index (if that entry has not been deleted in
        // the meantime by disposal)
        while (true) {
            Subscribers<T> x = subscribers.get();
            if (x.index == index && x.subscribers[index] == o) {
                boolean[] active = new boolean[x.active.length];
                System.arraycopy(x.active, 0, active, 0, active.length);
                int nextIndex = (index + 1) % active.length;
                while (nextIndex != index && !active[nextIndex]) {
                    nextIndex = (nextIndex + 1) % active.length;
                }
                active[nextIndex] = false;
                if (subscribers.compareAndSet(x,
                        new Subscribers<T>(x.subscribers, active, x.activeCount - 1, nextIndex, x.requested - 1))) {
                    oNext = x.subscribers[nextIndex];
                    break;
                }
            } else {
                // checkin because no active subscribers
                m.checkin();
                return false;
            }
        }
        Worker worker = scheduler.createWorker();
        worker.schedule(new Emitter<T>(worker, oNext, m));
        return true;
    }

    final class Releaser implements Runnable {

        private DecoratingMember<T> m;

        Releaser(DecoratingMember<T> m) {
            this.m = m;
        }

        @Override
        public void run() {
            m.disposeValue();
            release(m);
        }
    }

    final class Checker implements Runnable {

        private final DecoratingMember<T> m;

        Checker(DecoratingMember<T> m) {
            this.m = m;
        }

        @Override
        public void run() {
            log.debug("performing health check on {}", m);
            if (!pool.healthCheck.test(m.value())) {
                log.debug("failed health check");
                m.disposeValue();
                log.debug("scheduling recreation of member {}", m);
                scheduled.add(scheduler.schedule(() -> {
                    log.debug("recreating member after failed health check {}", m);
                    notInitialized.offer(m);
                    drain();
                }, pool.createRetryIntervalMs, TimeUnit.MILLISECONDS));
            } else {
                m.markAsChecked();
                initializedAvailable.offer(m);
                drain();
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }

    private void disposeAll() {
        initializedAvailable.clear();
        toBeReleased.clear();
        notInitialized.clear();
        disposeValues();
        removeAllSubscribers();
    }

    private void disposeValues() {
        scheduled.dispose();
        for (DecoratingMember<T> member : members) {
            member.disposeValue();
        }
    }

    void add(MonoMember.MemberMonoSubscriber<T> inner) {
        while (true) {
            Subscribers<T> a = subscribers.get();
            int n = a.subscribers.length;
            @SuppressWarnings("unchecked")
            MemberMonoSubscriber<T>[] b = new MemberMonoSubscriber[n + 1];
            System.arraycopy(a.subscribers, 0, b, 0, n);
            b[n] = inner;
            boolean[] active = new boolean[n + 1];
            System.arraycopy(a.active, 0, active, 0, n);
            active[n] = true;
            if (subscribers.compareAndSet(a, new Subscribers<T>(b, active, a.activeCount + 1, a.index, a.requested))) {
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void removeAllSubscribers() {
        while (true) {
            Subscribers<T> a = subscribers.get();
            if (subscribers.compareAndSet(a, EMPTY.withRequested(a.requested))) {
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void remove(MemberMonoSubscriber<T> inner) {
        while (true) {
            Subscribers<T> a = subscribers.get();
            int n = a.subscribers.length;
            if (n == 0) {
                return;
            }

            int j = -1;

            for (int i = 0; i < n; i++) {
                if (a.subscribers[i] == inner) {
                    j = i;
                    break;
                }
            }

            if (j < 0) {
                return;
            }
            Subscribers<T> next;
            if (n == 1) {
                next = EMPTY.withRequested(a.requested);
            } else {
                MemberMonoSubscriber<T>[] b = new MemberMonoSubscriber[n - 1];
                System.arraycopy(a.subscribers, 0, b, 0, j);
                System.arraycopy(a.subscribers, j + 1, b, j, n - j - 1);
                boolean[] active = new boolean[n - 1];
                System.arraycopy(a.active, 0, active, 0, j);
                System.arraycopy(a.active, j + 1, active, j, n - j - 1);
                int nextActiveCount = a.active[j] ? a.activeCount - 1 : a.activeCount;
                if (a.index >= j && a.index > 0) {
                    next = new Subscribers<T>(b, active, nextActiveCount, a.index - 1, a.requested);
                } else {
                    next = new Subscribers<T>(b, active, nextActiveCount, a.index, a.requested);
                }
            }
            if (subscribers.compareAndSet(a, next)) {
                break;
            }
        }
    }

    private static final class Subscribers<T> {
        final MemberMonoSubscriber<T>[] subscribers;
        // an subscriber is active until it is emitted to
        final boolean[] active;
        final int activeCount;
        final int index;
        final int requested;

        Subscribers(MemberMonoSubscriber<T>[] subscribers, boolean[] active, int activeCount, int index, int requested) {
            if (subscribers.length == 0 && index > 0) {
                throw new IllegalArgumentException("index must be -1 for zero length array");
            }
            if (subscribers.length != active.length) {
                throw new IllegalArgumentException();
            }
            this.subscribers = subscribers;
            this.index = index;
            this.active = active;
            this.activeCount = activeCount;
            this.requested = requested;
        }

        Subscribers<T> withRequested(int r) {
            return new Subscribers<T>(subscribers, active, activeCount, index, r);
        }
    }

    private static final class Emitter<T> implements Runnable {

        private final Worker worker;
        private final MemberMonoSubscriber<T> subscriber;
        private final Member<T> m;

        Emitter(Worker worker, MemberMonoSubscriber<T> subscriber, Member<T> m) {
            this.worker = worker;
            this.subscriber = subscriber;
            this.m = m;
        }

        @Override
        public void run() {
            worker.dispose();
            try {
                subscriber.success(m);
            } finally {
                subscriber.dispose();
            }
        }
    }

    public void release(DecoratingMember<T> m) {
        log.debug("adding released member to notInitialized queue {}", m);
        notInitialized.offer(m);
        drain();
    }

    static final class MemberMonoSubscriber<T> extends AtomicReference<MonoMember<T>> implements Subscription, Disposable {

        final CoreSubscriber<? super Member<T>> actual;

        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<MemberMonoSubscriber> STATE =
            AtomicIntegerFieldUpdater.newUpdater(MemberMonoSubscriber.class, "state");

        Member<T> value;

        static final int NO_REQUEST_HAS_VALUE  = 1;
        static final int HAS_REQUEST_NO_VALUE  = 2;
        static final int HAS_REQUEST_HAS_VALUE = 3;

        MemberMonoSubscriber(CoreSubscriber<? super Member<T>> actual, MonoMember<T> parent) {
            this.actual = actual;
            lazySet(parent);
        }

        public void success(Member<T> value) {
            for (; ; ) {
                int s = state;
                if (s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE) {
                    Operators.onNextDropped(value, actual.currentContext());
                    return;
                }
                if (s == HAS_REQUEST_NO_VALUE) {
                    if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
                        actual.onNext(value);
                        actual.onComplete();
                    }
                    return;
                }
                this.value = value;
                if (STATE.compareAndSet(this, s, NO_REQUEST_HAS_VALUE)) {
                    return;
                }
            }
        }

        public void error(Throwable e) {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
                actual.onError(e);
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                for (; ; ) {
                    int s = state;
                    if (s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE) {
                        return;
                    }
                    if (s == NO_REQUEST_HAS_VALUE) {
                        if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
                            actual.onNext(value);
                            actual.onComplete();
                        }
                        return;
                    }
                    if (STATE.compareAndSet(this, s, HAS_REQUEST_NO_VALUE)) {
                        return;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
                Member<T> old = value;
                value = null;
                Operators.onDiscard(old, actual.currentContext());
                dispose();
            }
        }

        @Override
        public void dispose() {
            MonoMember<T> parent = getAndSet(null);
            if (parent != null) {
                parent.remove(this);
                parent.drain();
            }
        }

        @Override
        public boolean isDisposed() {
            return get() == null;
        }
    }
}
