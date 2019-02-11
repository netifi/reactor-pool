package com.netifi.reactor.pool;

public interface PoolFactory<T> {

    Pool<T> create();
}
