package com.netifi.reactor.pool;

import reactor.core.publisher.Mono;

public interface Pool<T> extends AutoCloseable {

    Mono<Member<T>> member();
}