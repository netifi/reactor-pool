package com.netifi.reactor.pool;

public class PoolLimitException extends RuntimeException {
    private static final long serialVersionUID = -6902751635600001222L;

    public PoolLimitException() {
    }

    public PoolLimitException(int limit) {
        super("Requests limit exceeded: " + limit);
    }
}
