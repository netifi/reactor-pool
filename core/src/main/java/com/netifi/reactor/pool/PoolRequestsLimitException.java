package com.netifi.reactor.pool;

public class PoolRequestsLimitException extends RuntimeException {
    private static final long serialVersionUID = -6902751635600001222L;

    public PoolRequestsLimitException() {
    }

    public PoolRequestsLimitException(int limit) {
        super("Requests limit exceeded: " + limit);
    }
}
