package reactor.pool;

import java.util.function.Consumer;

final class Consumers {

    private Consumers() {
        // prevent instantiation
    }

    static final class DoNothingHolder {
        static final Consumer<Object> value = new Consumer<Object>() {

            @Override
            public void accept(Object arg0) {
                // do nothing
            }

        };
    }

    @SuppressWarnings("unchecked")
    static <T> Consumer<T> doNothing() {
        return (Consumer<T>) DoNothingHolder.value;
    }

}
