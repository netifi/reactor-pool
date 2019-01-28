package com.netifi.reactor.pool;

import com.netifi.reactor.pool.r2dbc.CloseableConnectionFactory;
import com.netifi.reactor.pool.r2dbc.PooledConnectionFactory;
import com.netifi.reactor.pool.r2dbc.RdbcPoolUtils;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class PostgresConnectionPoolFactoryTest extends BaseTest {

    @BeforeAll
    static void init() {
        Hooks.onNextDropped(RdbcPoolUtils::checkinMember);
    }

    @Test
    void poolConnections() {
        CloseableConnectionFactory connectionFactory = pooledConnectionFactory();
        Mono<? extends Connection> connF =
                Mono.defer(() -> Mono.from(connectionFactory.create()));
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
