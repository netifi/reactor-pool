package com.netifi.reactor.pool;

import com.netifi.reactor.pool.r2dbc.R2DbcPoolManager;
import io.r2dbc.postgresql.PostgresqlConnection;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.function.Function;
import java.util.stream.Stream;

public class PostgresConnectionPoolTest extends BaseTest {
    private static final Logger logger = LoggerFactory
            .getLogger(PostgresConnectionPoolTest.class);

    @BeforeAll
    static void init() {
        Hooks.onNextDropped(PoolUtils::checkinMember);
    }

    @ParameterizedTest
    @MethodSource("poolsProvider")
    void poolConnections(Function<PostgreSQLContainer, Pool<PostgresqlConnection>> poolF) {
        Pool<PostgresqlConnection> pool = poolF.apply(postgreSQLContainer);
        Flux<Integer> queryThenCheckin = pool
                .member()
                .flatMapMany(
                        m -> m.value()
                                .createStatement("select 1 c")
                                .execute()
                                .delayElements(Duration.ofMillis(2))
                                .flatMap(res ->
                                        res.map(
                                                (r, md) -> r.get("c", Integer.class))
                                )
                                .doFinally(s -> m.checkin())
                );

        runAndVerify(queryThenCheckin, () -> close(pool));
    }

    static Stream<Arguments> poolsProvider() {
        Function<PostgreSQLContainer, Pool<PostgresqlConnection>> nonBlockingPool =
                container ->
                        createNonblockingPool(
                                validPostgresConfig(container),
                                POOL_SIZE);

        Function<PostgreSQLContainer, Pool<PostgresqlConnection>> threadLocalPool =
                container ->
                        createPartitionedPool(
                                validPostgresConfig(container));

        return Stream.of(
                Arguments.of(nonBlockingPool),
                Arguments.of(threadLocalPool)
        );
    }

    @Test
    void poolConnectionsInvalidAddress() {
        NonBlockingPool<PostgresqlConnection> pool = createNonblockingPool(
                invalidPostgresConfig(),
                POOL_SIZE);
        Mono<Member<PostgresqlConnection>> member = pool.member();

        member.retryWhen(err ->
                err.flatMap(e -> {
                    if (pool.members.isEmpty()) {
                        return Mono.error(new IllegalStateException(
                                "Pool members are not checked in on error"));
                    } else {
                        return Mono.just(e);
                    }
                })
                        .timeout(Duration.ofSeconds(2))
                        .flatMap(e -> Mono.delay(Duration.ofMillis(10)))
        ).take(Duration.ofSeconds(10))
                .flatMap(m ->
                        Mono.error(
                                new IllegalStateException(
                                        "Received member while connecting wrong address")))
                .as(StepVerifier::create)
                .expectComplete()
                .verify();
    }

    private static void close(Pool<PostgresqlConnection> pool) {
        try {
            pool.close();
        } catch (Exception e) {
            logger.error("Error closing pool", e);
        }
    }

    @NotNull
    private static Pool<PostgresqlConnection> createPartitionedPool(
            PostgresqlConnectionConfiguration config) {
        PostgresqlConnectionFactory connectionFactory =
                new PostgresqlConnectionFactory(config);
        R2DbcPoolManager<PostgresqlConnection> poolManager =
                new R2DbcPoolManager<>(connectionFactory);
        NonBlockingPoolFactory<PostgresqlConnection> poolFactory =
                new NonBlockingPoolFactory<>(
                        POOL_SIZE,
                        MAX_PENDING_REQUESTS_COUNT,
                        POOLED_VALIDATION_INTERVAL,
                        POOLED_TIMEOUT,
                        poolManager);

        return new PartitionedThreadPool<>(poolFactory);
    }

    @NotNull
    private static NonBlockingPool<PostgresqlConnection> createNonblockingPool(PostgresqlConnectionConfiguration config,
                                                                               int poolSize) {
        PostgresqlConnectionFactory connectionFactory =
                new PostgresqlConnectionFactory(config);
        R2DbcPoolManager<PostgresqlConnection> poolManager =
                new R2DbcPoolManager<>(connectionFactory);
        return new NonBlockingPool<>(poolSize,
                MAX_PENDING_REQUESTS_COUNT,
                POOLED_VALIDATION_INTERVAL,
                POOLED_TIMEOUT,
                poolManager);
    }
}
