package com.netifi.reactor.pool.r2dbc;

import com.netifi.reactor.pool.PoolManager;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class R2DbcPoolManager<T extends Connection> implements PoolManager<T> {
    private final ConnectionFactory connectionFactory;

    public R2DbcPoolManager(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Publisher<T> create() {
        return Mono.from(connectionFactory.create())
                .map(c -> ((T) c));
    }

    @Override
    public void dispose(T pooled) {
        Mono.from(pooled.close()).subscribe();
    }

    @Override
    public Publisher<Void> isAlive(T pooled) {
        return Mono.from(pooled.createStatement("select 1")
                .execute())
                .then();
    }
}
