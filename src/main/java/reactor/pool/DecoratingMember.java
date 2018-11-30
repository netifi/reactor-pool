package reactor.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

final class DecoratingMember<T> implements Member<T> {

    private static final Logger log = LoggerFactory.getLogger(DecoratingMember.class);

    private volatile T value;
    private final MonoMember<T> monoMember;
    private final BiFunction<? super T, ? super Checkin, ? extends T> checkinDecorator;

    // synchronized by MonoMember.drain() wip
    private Disposable scheduled;

    // synchronized by MonoMember.drain() wip
    private boolean releasing;

    // synchronized by MonoMember.drain() wip
    private boolean checking;

    // synchronized by MonoMember.drain() wip
    // not subject to word tearing, because of ordering in drain loop (will only
    // read this value if check has finished)
    private long lastCheckTime;

    DecoratingMember(T value, BiFunction<? super T, ? super Checkin, ? extends T> checkinDecorator,
            MonoMember<T> monoMember) {
        this.checkinDecorator = checkinDecorator;
        this.monoMember = monoMember;
        this.value = value;
    }

    @Override
    public T value() {
        return checkinDecorator.apply(value, this);
    }

    @Override
    public void checkin() {
        monoMember.pool.checkin(this);
    }

    public void markAsReleasing() {
        this.releasing = true;
    }

    public boolean isReleasing() {
        return releasing;
    }

    public void markAsChecking() {
        this.checking = true;
    }

    public boolean isChecking() {
        return checking;
    }

    @Override
    public void disposeValue() {
        try {
            if (scheduled != null) {
                scheduled.dispose();
                scheduled = null;
            }
            log.debug("disposing value {}", value);
            monoMember.pool.disposer.accept(value);
        } finally {
            value = null;
            checking = false;
        }
    }

    public void setValueAndClearReleasingFlag(T value) {
        this.value = value;
        this.releasing = false;
        this.lastCheckTime = now();
    }

    void scheduleRelease() {
        if (scheduled != null) {
            scheduled.dispose();
            log.debug("cancelled scheduled release of {}", this);
        }
        long maxIdleTimeMs = monoMember.pool.maxIdleTimeMs;
        if (maxIdleTimeMs > 0) {
            // TODO make `this` runnable to save lambda allocation
            scheduled = monoMember.pool.scheduler.schedule(() -> {
                monoMember.addToBeReleased(this);
            }, maxIdleTimeMs, TimeUnit.MILLISECONDS);
            log.debug("scheduled release in {}ms of {}", maxIdleTimeMs, this);
        }
    }

    @Override
    public String toString() {
        return "DecoratingMember [value=" + value + "]";
    }

    public void markAsChecked() {
        checking = false;
        lastCheckTime = now();
    }

    private long now() {
        return monoMember.pool.scheduler.now(TimeUnit.MILLISECONDS);
    }

    public long lastCheckTime() {
        return lastCheckTime;
    }

}
