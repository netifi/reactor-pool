package com.netifi.reactor.pool;

public interface Member<T> extends Checkin {

    T value();
}
