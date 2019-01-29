package com.netifi.reactor.pool.r2dbc;

import com.netifi.reactor.pool.NonBlockingPoolFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;

import java.time.Duration;

public class PooledFactoryBuilder<T extends Connection> {
    private ConnectionFactory connectionFactory;
    private int poolSize = 10;
    private int maxPendingRequestsCount = 100;
    private Duration pooledValidationInterval = Duration.ofSeconds(30);
    private Duration pooledTimeout = Duration.ofSeconds(5);

    public PooledFactoryBuilder<T> connectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        return this;
    }

    public PooledFactoryBuilder<T> poolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    public PooledFactoryBuilder<T> maxPendingRequests(int maxPendingRequests) {
        this.maxPendingRequestsCount = maxPendingRequests;
        return this;
    }

    public PooledFactoryBuilder<T> pooledValidationInterval(Duration pooledValidationInterval) {
        this.pooledValidationInterval = pooledValidationInterval;
        return this;
    }

    public PooledFactoryBuilder<T> pooledTimeout(Duration pooledTimeout) {
        this.pooledTimeout = pooledTimeout;
        return this;
    }

    public CloseableConnectionFactory build() {
        return new PooledConnectionFactory<T>(
                connectionFactory,
                cf -> new NonBlockingPoolFactory<>(
                        poolSize,
                        maxPendingRequestsCount,
                        pooledValidationInterval,
                        pooledTimeout,
                        new R2DbcPoolManager<>(cf)));
    }
}
