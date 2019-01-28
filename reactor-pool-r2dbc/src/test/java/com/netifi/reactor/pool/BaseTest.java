package com.netifi.reactor.pool;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;

class BaseTest {
    static final int POOL_SIZE = 5;
    static final int MAX_PENDING_REQUESTS_COUNT = 1500;
    static final Duration POOLED_VALIDATION_INTERVAL = Duration.ofSeconds(5);
    static final Duration POOLED_TIMEOUT = Duration.ofSeconds(5);

    final PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer();

    @BeforeEach
    void setUp() {
        postgreSQLContainer.start();
    }

    @AfterEach
    void tearDown() {
        postgreSQLContainer.stop();
    }

    @NotNull
    PostgresqlConnectionConfiguration invalidPostgresConfig() {
        return PostgresqlConnectionConfiguration.builder()
                .applicationName("test")
                .database("invalid")
                .host("localhost")
                .port(5433)
                .username("invalid")
                .password("invalid")
                .build();
    }

    static PostgresqlConnectionConfiguration validPostgresConfig(
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

    static void runAndVerify(Flux<Integer> queryThenCheckin,
                             Runnable close) {
        Flux.interval(Duration.ofMillis(1))
                .onBackpressureDrop()
                .parallel(Runtime.getRuntime().availableProcessors())
                .runOn(Schedulers.elastic())
                .flatMap(v -> queryThenCheckin,
                        false,
                        MAX_PENDING_REQUESTS_COUNT / 2)
                .sequential()
                .onErrorResume(err -> {
                    if (err instanceof PoolLimitException) {
                        return Mono.empty();
                    } else {
                        return Mono.error(err);
                    }
                })
                .timeout(Duration.ofSeconds(5))
                .take(Duration.ofSeconds(60))
                .then()
                .doFinally(s -> close.run())
                .as(StepVerifier::create)
                .expectComplete()
                .verify();

    }
}
