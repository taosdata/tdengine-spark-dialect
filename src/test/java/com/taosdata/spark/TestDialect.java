package com.taosdata.spark;

/**
 * Shared dialect instance for the integration tests. JdbcDialects dedupes registrations by
 * object identity, so registering a fresh instance per test class would leave two matching
 * dialects in the registry and make JdbcDialects.get() return an AggregatedDialect wrapper.
 */
final class TestDialect {

    static final TDengineDialect INSTANCE = new TDengineDialect();

    private TestDialect() {
    }
}
