package com.netifi.reactor.pool;

class Utils {
    static int requirePositive(int size) {
        if (size > 0) {
            return size;
        } else {
            throw new IllegalArgumentException(
                    String.format("Must be positive: %d", size));
        }
    }
}
