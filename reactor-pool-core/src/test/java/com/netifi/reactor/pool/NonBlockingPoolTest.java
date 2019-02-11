package com.netifi.reactor.pool;

import com.netifi.reactor.pool.TestPoolManager.Resource;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

public class NonBlockingPoolTest {
    private static final int POOL_SIZE = 5;
    private static final int MAX_PENDING_REQUESTS_COUNT = 1500;
    private static final Duration POOLED_VALIDATION_INTERVAL = Duration.ofSeconds(5);
    private static final Duration POOLED_TIMEOUT = Duration.ofSeconds(1);
    private NonBlockingPool<Resource> pool;

    @BeforeAll
    static void init() {
        Hooks.onNextDropped(PoolUtils::checkinMember);
    }

    @BeforeEach
    void setUp() {
        pool = new NonBlockingPool<>(POOL_SIZE,
                MAX_PENDING_REQUESTS_COUNT,
                POOLED_VALIDATION_INTERVAL,
                POOLED_TIMEOUT,
                new TestPoolManager());

    }

    @AfterEach
    void tearDown() {
        pool.close();
    }

    @Test
    void checkInAfterDispose() {
        Mono<Member<Resource>> memberMono = pool.member();
        Member<Resource> member = memberMono.block();
        Resource resource = member.value();
        pool.close();
        member.checkin();
        Assertions.assertTrue(resource.isDisposed());
    }

    @Test
    void checkoutAfterDispose() {
        pool.close();
        pool.member()
                .as(StepVerifier::create)
                .expectError()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void pendingMembersSubscribeBeforeClose() {
        NonBlockingPool<Resource> oneElementPool =
                new NonBlockingPool<>(1,
                        MAX_PENDING_REQUESTS_COUNT,
                        POOLED_VALIDATION_INTERVAL,
                        POOLED_TIMEOUT,
                        new TestPoolManager());
        oneElementPool.member().subscribe();
        Mono<Member<Resource>> pendingMember = oneElementPool.member();
        Mono.delay(Duration.ofSeconds(1)).subscribe(v -> {
            try {
                oneElementPool.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        pendingMember
                .as(StepVerifier::create)
                .expectError()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void pendingMembersSubscribeAfterClose() {
        NonBlockingPool<Resource> oneElementPool =
                new NonBlockingPool<>(1,
                        MAX_PENDING_REQUESTS_COUNT,
                        POOLED_VALIDATION_INTERVAL,
                        POOLED_TIMEOUT,
                        new TestPoolManager());

        oneElementPool.member().subscribe();
        Mono<Member<Resource>> pendingMember = oneElementPool.member();
        oneElementPool.close();
        pendingMember
                .as(StepVerifier::create)
                .expectError()
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void memberAfterPoolClose() {
        pool.close();
        pool.member()
                .as(StepVerifier::create)
                .expectError(PoolClosedException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void doubleCheckin() {
        Member<Resource> member = pool.member().block();
        member.checkin();
        Assertions.assertThrows(IllegalStateException.class, member::checkin);
    }

    @Test
    void pendingRequestsCountLimit() {
        NonBlockingPool<Resource> pool = new NonBlockingPool<>(1,
                1,
                POOLED_VALIDATION_INTERVAL,
                POOLED_TIMEOUT,
                new TestPoolManager());
        Mono<Member<Resource>> m = pool.member();
        Assertions.assertThrows(PoolLimitException.class,
                () -> Flux.merge(m, m, m).blockLast());
    }
}
