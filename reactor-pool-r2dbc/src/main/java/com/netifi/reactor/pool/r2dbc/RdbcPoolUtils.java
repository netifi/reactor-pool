package com.netifi.reactor.pool.r2dbc;

import com.netifi.reactor.pool.PoolUtils;

public class RdbcPoolUtils {

    /*Should be used in Hooks.onErrorDropped*/
    public static void checkinMember(Object member) {
        PoolUtils.checkinMember(member);
        if (member instanceof PooledConnection) {
            ((PooledConnection) member).close().subscribe();
        }
    }
}
