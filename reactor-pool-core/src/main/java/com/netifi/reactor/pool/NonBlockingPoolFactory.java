package com.netifi.reactor.pool;

import java.time.Duration;
import java.util.Objects;

public class NonBlockingPoolFactory<T> implements PoolFactory<T> {
    private final int poolSize;
    private int maxPendingRequestsCount;
    private Duration pooledValidationInterval;
    private Duration pooledTimeout;
    private final PoolManager<T> poolManager;

    public NonBlockingPoolFactory(int poolSize,
                                  int maxPendingRequestsCount,
                                  Duration pooledValidationInterval,
                                  Duration pooledTimeout,
                                  PoolManager<T> poolManager) {
        this.poolSize = requirePositive(poolSize);
        this.maxPendingRequestsCount = requirePositive(maxPendingRequestsCount);
        this.pooledValidationInterval = Objects.requireNonNull(pooledValidationInterval);
        this.pooledTimeout = Objects.requireNonNull(pooledTimeout);
        this.poolManager = Objects.requireNonNull(poolManager);
    }

    @Override
    public NonBlockingPool<T> create() {
        return new NonBlockingPool<>(
                poolSize,
                maxPendingRequestsCount,
                pooledValidationInterval,
                pooledTimeout,
                poolManager);
    }

    private static int requirePositive(int value) {
        if (value > 0) {
            return value;
        } else {
            throw new IllegalArgumentException(
                    String.format("Value must be positive: %d", value));
        }
    }

}
