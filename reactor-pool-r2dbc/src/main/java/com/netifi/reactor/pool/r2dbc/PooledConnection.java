package com.netifi.reactor.pool.r2dbc;

import com.netifi.reactor.pool.Checkin;
import com.netifi.reactor.pool.Member;
import com.netifi.reactor.pool.PoolClosedException;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

class PooledConnection extends  AtomicBoolean implements Connection {
    private static final RuntimeException closedError = new PoolClosedException();
    private static final Mono<?> closedMono = Mono.error(closedError);

    private final Connection source;
    private final Checkin member;

    public PooledConnection(Member<? extends Connection> member) {
        this.source = member.value();
        this.member = member;
    }

    @Override
    public Publisher<Void> beginTransaction() {
        return isOpen().thenMany(source.beginTransaction());
    }

    @Override
    public Mono<Void> close() {
        if (get()) {
            return Mono.empty();
        } else {
            return Mono.fromRunnable(() -> {
                if (compareAndSet(false, true)) {
                    member.checkin();
                }
            });
        }
    }

    @Override
    public Publisher<Void> commitTransaction() {
        return isOpen().thenMany(source.commitTransaction());
    }

    @Override
    public Batch<?> createBatch() {
        return doCreateBatch();
    }

    @Override
    public Publisher<Void> createSavepoint(String name) {
        return isOpen().thenMany(source.createSavepoint(name));
    }

    @Override
    public Statement<?> createStatement(String sql) {
        return doCreateStatement(sql);
    }

    @Override
    public Publisher<Void> releaseSavepoint(String name) {
        return isOpen().thenMany(source.releaseSavepoint(name));
    }

    @Override
    public Publisher<Void> rollbackTransaction() {
        return isOpen().thenMany(source.rollbackTransaction());
    }

    @Override
    public Publisher<Void> rollbackTransactionToSavepoint(String name) {
        return isOpen().thenMany(source.rollbackTransactionToSavepoint(name));
    }

    @Override
    public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        return isOpen().thenMany(source.setTransactionIsolationLevel(isolationLevel));
    }

    private Batch<?> doCreateBatch() {
        if (get()) {
            throw closedError;
        } else {
            return source.createBatch();
        }
    }

    private Statement<?> doCreateStatement(String sql) {
        if (get()) {
            throw closedError;
        } else {
            return source.createStatement(sql);
        }
    }

    private Mono<Void> isOpen() {
        return Mono.defer(() -> {
            if (get()) {
                return closedPublisher();
            } else {
                return Mono.empty();
            }
        });
    }

    private static <T> T closedPublisher() {
        return (T) closedMono;
    }
}
