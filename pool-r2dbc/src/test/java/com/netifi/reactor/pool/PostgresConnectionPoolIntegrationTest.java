package com.netifi.reactor.pool;

import com.netifi.reactor.pool.r2dbc.R2DbcPoolManager;
import io.r2dbc.postgresql.PostgresqlConnection;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.function.Function;
import java.util.stream.Stream;

public class PostgresConnectionPoolIntegrationTest {
    private static final Logger logger = LoggerFactory
            .getLogger(PostgresConnectionPoolIntegrationTest.class);
    private static final int POOL_SIZE = 5;
    private static final int MAX_PENDING_REQUESTS_COUNT = 1500;
    private static final Duration POOLED_VALIDATION_INTERVAL = Duration.ofSeconds(5);
    private static final Duration POOLED_TIMEOUT = Duration.ofSeconds(1);

    private final PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer();

    @BeforeAll
    static void init() {
        Hooks.onNextDropped(PoolUtils::checkinMember);
    }

    @BeforeEach
    void setUp() {
        postgreSQLContainer.start();
    }

    @AfterEach
    void tearDown() {
        postgreSQLContainer.stop();
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

        Flux.interval(Duration.ofMillis(1))
                .onBackpressureDrop()
                .parallel(Runtime.getRuntime().availableProcessors())
                .runOn(Schedulers.elastic())
                .flatMap(v -> queryThenCheckin,
                        false,
                        MAX_PENDING_REQUESTS_COUNT * 3 / 4)
                .sequential()
                .timeout(Duration.ofSeconds(5))
                .take(Duration.ofSeconds(60))
                .then()
                .doFinally(s -> close(pool))
                .as(StepVerifier::create)
                .expectComplete()
                .verify();
    }

    static Stream<Arguments> poolsProvider() {
        Function<PostgreSQLContainer, Pool<PostgresqlConnection>> nonBlockingPool =
                container ->
                        createNonblockingPool(
                                validPostgresConfig(container),
                                POOL_SIZE);

        Function<PostgreSQLContainer, Pool<PostgresqlConnection>> threadLocalPool =
                container ->
                        createThreadLocalPool(
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
    private static Pool<PostgresqlConnection> createThreadLocalPool(
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

    @NotNull
    private PostgresqlConnectionConfiguration invalidPostgresConfig() {
        return PostgresqlConnectionConfiguration.builder()
                .applicationName("test")
                .database("invalid")
                .host("localhost")
                .port(5433)
                .username("invalid")
                .password("invalid")
                .build();
    }

    private static PostgresqlConnectionConfiguration validPostgresConfig(
            PostgreSQLContainer container) {
        JdbcUrlParser.Url url = JdbcUrlParser.parse(container.getJdbcUrl());
        return PostgresqlConnectionConfiguration.builder()
                .applicationName("test")
                .database(url.getPath())
                .host(url.getHost())
                .port(url.getPort())
                .username(container.getUsername())
                .password(container.getPassword())
                .build();
    }
}
