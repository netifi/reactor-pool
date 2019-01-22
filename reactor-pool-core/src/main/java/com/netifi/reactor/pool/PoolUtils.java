package com.netifi.reactor.pool;

public class PoolUtils {
    /*Should be used in Hooks.onErrorDropped*/
    public static void checkinMember(Object member) {
        if (member instanceof Checkin) {
            ((Checkin) member).checkin();
        }
    }
}
