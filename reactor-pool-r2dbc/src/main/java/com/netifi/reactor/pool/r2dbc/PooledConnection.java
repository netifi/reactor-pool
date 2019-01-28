package com.netifi.reactor.pool.r2dbc;

import com.netifi.reactor.pool.Checkin;
import com.netifi.reactor.pool.Member;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

class PooledConnection implements Connection {
    private final Connection source;
    private final Checkin member;
    private final AtomicBoolean checkin = new AtomicBoolean();

    public PooledConnection(Member<? extends Connection> member) {
        this.source = member.value();
        this.member = member;
    }

    @Override
    public Publisher<Void> beginTransaction() {
        return source.beginTransaction();
    }

    @Override
    public Mono<Void> close() {
        return Mono.fromRunnable(() -> {
            if (checkin.compareAndSet(false, true)) {
                member.checkin();
            }
        });
    }

    @Override
    public Publisher<Void> commitTransaction() {
        return source.commitTransaction();
    }

    @Override
    public Batch<?> createBatch() {
        return source.createBatch();
    }

    @Override
    public Publisher<Void> createSavepoint(String name) {
        return source.createSavepoint(name);
    }

    @Override
    public Statement<?> createStatement(String sql) {
        return source.createStatement(sql);
    }

    @Override
    public Publisher<Void> releaseSavepoint(String name) {
        return source.releaseSavepoint(name);
    }

    @Override
    public Publisher<Void> rollbackTransaction() {
        return source.rollbackTransaction();
    }

    @Override
    public Publisher<Void> rollbackTransactionToSavepoint(String name) {
        return source.rollbackTransactionToSavepoint(name);
    }

    @Override
    public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        return source.setTransactionIsolationLevel(isolationLevel);
    }
}
