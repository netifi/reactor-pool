package com.netifi.reactor.pool.r2dbc;

import io.r2dbc.spi.ConnectionFactory;

public interface CloseableConnectionFactory extends ConnectionFactory {

    void close();
}
