package com.netifi.reactor.pool;

import java.net.URI;

class JdbcUrlParser {
    private static final String prefix = "jdbc:";

    public static Url parse(String url) {
        if (url.startsWith(prefix)) {
            String suffix = url.substring(prefix.length());
            URI uri = URI.create(suffix);
            String uriPath = uri.getPath();
            String scheme = uri.getScheme();
            String host = uri.getHost();
            int port = uri.getPort();
            if (!isPortSet(port) || isAnyNull(uriPath, scheme, host)) {
                throw new IllegalArgumentException(
                        String.format("Url is malformed: %s", url)
                );
            }
            String path = uriPath.substring(uriPath.indexOf("/") + 1);
            return new Url(
                    scheme,
                    host,
                    port,
                    path);
        } else {
            throw new IllegalArgumentException(
                    String.format("Url %s does not start with prefix: %s",
                            url,
                            prefix));
        }
    }

    private static boolean isPortSet(int port) {
        return port > 0;
    }

    private static boolean isAnyNull(String... parts) {
        for (int i = 0; i < parts.length; i++) {
            if (parts[i] == null) {
                return true;
            }
        }
        return false;
    }

    public static class Url {
        private final String scheme;
        private final String host;
        private final int port;
        private final String path;

        public Url(String scheme, String host, int port, String path) {
            this.scheme = scheme;
            this.host = host;
            this.port = port;
            this.path = path;
        }

        public String getScheme() {
            return scheme;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getPath() {
            return path;
        }
    }
}
