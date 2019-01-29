package com.netifi.reactor.pool.r2dbc;

import com.netifi.reactor.pool.*;
import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;

import java.util.function.Function;

public class PooledConnectionFactory<T extends Connection> implements CloseableConnectionFactory {
    private final ConnectionFactory connectionFactory;
    private final PartitionedThreadPool<T> pool;

    public PooledConnectionFactory(ConnectionFactory connectionFactory,
                                   Function<ConnectionFactory, PoolFactory<T>> f) {
        this.connectionFactory = connectionFactory;
        this.pool = new PartitionedThreadPool<>(f.apply(connectionFactory));
    }

    @Override
    public Publisher<? extends Connection> create() {
        return pool.member().map(PooledConnection::new);
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return connectionFactory.getMetadata();
    }

    @Override
    public void close() {
        pool.close();
    }

    public static <T extends Connection> PooledFactoryBuilder<T> builder() {
        return new PooledFactoryBuilder<>();
    }
}
