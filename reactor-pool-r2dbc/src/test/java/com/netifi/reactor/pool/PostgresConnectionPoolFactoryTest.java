package com.netifi.reactor.pool;

import com.netifi.reactor.pool.r2dbc.CloseableConnectionFactory;
import com.netifi.reactor.pool.r2dbc.PooledConnectionFactory;
import com.netifi.reactor.pool.r2dbc.RdbcPoolUtils;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.Function;

public class PostgresConnectionPoolFactoryTest extends BaseTest {

    @BeforeAll
    static void init() {
        Hooks.onNextDropped(RdbcPoolUtils::checkinMember);
    }

    @Test
    void poolConnections() {
        CloseableConnectionFactory connectionFactory = pooledConnectionFactory();
        Mono<? extends Connection> connF =
                Mono.defer(
                        () -> Mono.from(connectionFactory.create())
                );
        Flux<Integer> queryThenCheckin = connF
                .flatMapMany(
                        c -> Flux.from(c.createStatement("select 1 c")
                                .execute())
                                .delayElements(Duration.ofMillis(2))
                                .flatMap(res ->
                                        res.map(
                                                (r, md) -> r.get("c", Integer.class))
                                )
                                .doFinally(s -> Mono.from(c.close()).subscribe())
                );

        runAndVerify(queryThenCheckin, connectionFactory::close);
    }

    @Test
    void pooledConnectionAfterClose() {
        CloseableConnectionFactory connectionFactory = pooledConnectionFactory();
        Connection connection = Mono.from(connectionFactory.create()).block();
        Mono.from(connection.close()).subscribe();
        connectionCalls()
                .forEach(call ->
                        Flux.from(call.apply(connection))
                                .doFinally(s -> connectionFactory.close())
                                .as(StepVerifier::create)
                                .expectError(PoolClosedException.class)
                                .verify(Duration.ofSeconds(5)));
    }

    static Iterable<Function<Connection, Publisher<?>>> connectionCalls() {

        Function<Connection, Publisher<?>> createStatement =
                connection ->
                        Mono.fromRunnable(() -> connection
                                .createStatement("select 1 c"));

        Function<Connection, Publisher<?>> createBatch =
                connection ->
                        Mono.fromRunnable(connection::createBatch);

        Function<Connection, Publisher<?>> savePoint =
                connection ->
                        connection.createSavepoint("test");

        Function<Connection, Publisher<?>> releaseSavepoint =
                connection ->
                        connection.releaseSavepoint("test");

        Function<Connection, Publisher<?>> beginTransaction =
                Connection::beginTransaction;

        Function<Connection, Publisher<?>> commitTransaction =
                Connection::commitTransaction;

        Function<Connection, Publisher<?>> rollbackTransaction =
                Connection::rollbackTransaction;

        Function<Connection, Publisher<?>> transactionIsolationLevel =
                connection ->
                        connection.setTransactionIsolationLevel(
                                IsolationLevel.READ_COMMITTED);

        Function<Connection, Publisher<?>> rollbackTransactionToSavepoint =
                connection ->
                        connection.rollbackTransactionToSavepoint("test");

        return Arrays.asList(
                createStatement,
                createBatch,
                savePoint,
                releaseSavepoint,
                beginTransaction,
                commitTransaction,
                rollbackTransaction,
                transactionIsolationLevel,
                rollbackTransactionToSavepoint
        );
    }

    private CloseableConnectionFactory pooledConnectionFactory() {
        PostgresqlConnectionConfiguration config =
                validPostgresConfig(postgreSQLContainer);

        PostgresqlConnectionFactory connectionFactory =
                new PostgresqlConnectionFactory(config);

        return PooledConnectionFactory.builder()
                .connectionFactory(connectionFactory)
                .maxPendingRequests(MAX_PENDING_REQUESTS_COUNT)
                .pooledTimeout(POOLED_TIMEOUT)
                .pooledValidationInterval(POOLED_VALIDATION_INTERVAL)
                .poolSize(POOL_SIZE)
                .build();
    }
}
