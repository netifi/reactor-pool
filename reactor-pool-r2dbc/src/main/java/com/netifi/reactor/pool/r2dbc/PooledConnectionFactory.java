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
        PoolFactory<T> poolFactory = f.apply(connectionFactory);
        this.pool = new PartitionedThreadPool<>(poolFactory);
    }

    @Override
    public Publisher<? extends Connection> create() {
        return pool.member().map(PooledConnection::new);
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return connectionFactory.getMetadata();
    }

    public static <T extends Connection> PooledFactoryBuilder<T> builder() {
        return new PooledFactoryBuilder<>();
    }

    @Override
    public void close() {
        pool.close();
    }
}
