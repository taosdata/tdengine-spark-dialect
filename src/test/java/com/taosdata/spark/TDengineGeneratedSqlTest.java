package com.taosdata.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Verifies that the SQL Spark generates by default for its JDBC data source
 * (schema probe, table-existence probe, pushed-down filters, CREATE/DROP TABLE and
 * INSERT) executes correctly against TDengine. Skipped when no local server is reachable.
 */
public class TDengineGeneratedSqlTest {

    private static final String BASE_URL =
            System.getProperty("tdengine.ws.url", "jdbc:TAOS-WS://127.0.0.1:6041/");
    private static final String DB = "spark_sql_gen_test";
    private static final String TABLE = DB + ".meters";
    private static final long TS1 = 1756713600000L;
    private static final long TS2 = 1756713601000L;
    private static final long TS3 = 1756713602000L;

    private static SparkSession spark;
    private static Properties connProps;

    @BeforeClass
    public static void setUp() throws Exception {
        connProps = new Properties();
        connProps.setProperty("user", "root");
        connProps.setProperty("password", "taosdata");

        Connection conn;
        try {
            conn = DriverManager.getConnection(BASE_URL, connProps);
        } catch (Throwable t) {
            Assume.assumeNoException("cannot reach TDengine at " + BASE_URL + ", skipping integration tests", t);
            return;
        }
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + DB);
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (" +
                    "ts TIMESTAMP, voltage INT, location NCHAR(64), note VARCHAR(100), active BOOL)");
            stmt.execute("INSERT INTO " + TABLE + " (ts, voltage, location, note, active) VALUES " +
                    "(" + TS1 + ", 220, 'loc-1', 'note-1', true), " +
                    "(" + TS2 + ", 221, 'loc-2', NULL, false), " +
                    "(" + TS3 + ", 222, NULL, 'note-3', true)");
        } finally {
            conn.close();
        }

        JdbcDialects.registerDialect(TestDialect.INSTANCE);

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("TDengineGeneratedSqlTest")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterClass
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
        if (connProps != null) {
            try (Connection conn = DriverManager.getConnection(BASE_URL, connProps);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP DATABASE IF EXISTS " + DB);
            } catch (Throwable ignored) {
                // server not reachable, nothing to clean up
            }
        }
    }

    private Dataset<Row> readTable(String table) {
        return spark.read()
                .format("jdbc")
                .option("url", BASE_URL)
                .option("dbtable", table)
                .option("user", "root")
                .option("password", "taosdata")
                .load();
    }

    @Test
    public void testTableExistsQuery() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL, connProps);
             Statement stmt = conn.createStatement()) {
            // existing table: the probe query must execute without error
            stmt.executeQuery("SELECT * FROM " + TABLE + " LIMIT 0").close();
            // missing table: the probe query must fail, which is how Spark's JdbcUtils
            // concludes that the table does not exist
            try {
                stmt.executeQuery("SELECT * FROM " + DB + ".no_such_table LIMIT 0");
                fail("expected an exception for a missing table");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage() != null);
            }
        }
    }

    @Test
    public void testPushedDownFilters() {
        Dataset<Row> df = readTable(TABLE);

        // numeric comparison and equality
        assertEquals(2, df.filter("voltage > 220").count());
        assertEquals(1, df.filter("voltage = 220").count());
        // equality on an NCHAR column
        assertEquals(1, df.filter("location = 'loc-1'").count());
        // NULL handling on NCHAR and VARCHAR columns
        assertEquals(1, df.filter("location IS NULL").count());
        assertEquals(2, df.filter("location IS NOT NULL").count());
        assertEquals(1, df.filter("note IS NULL").count());
        // IN, OR, AND, boolean literal
        assertEquals(2, df.filter("voltage IN (220, 221)").count());
        assertEquals(2, df.filter("voltage = 220 OR voltage = 221").count());
        assertEquals(1, df.filter("active = true AND voltage > 220").count());
        // string pattern pushdown: startsWith compiles to LIKE 'loc%'
        assertEquals(2, df.filter(df.col("location").startsWith("loc")).count());

        // verify actual values through a pushed filter, not only counts
        List<Row> rows = df.filter("voltage = 221").collectAsList();
        assertEquals(1, rows.size());
        assertEquals(new Timestamp(TS2), rows.get(0).getTimestamp(0));
        assertEquals("loc-2", rows.get(0).getString(2));
        assertTrue(rows.get(0).isNullAt(3));
        assertTrue(!rows.get(0).getBoolean(4));
    }

    @Test
    public void testSparkCreateAndOverwriteTable() {
        String created = DB + ".created_by_spark";

        // all fields nullable (TDengine has no NOT NULL constraint syntax),
        // first column TIMESTAMP as required by TDengine
        StructType schema = new StructType()
                .add("ts", DataTypes.TimestampType)
                .add("b", DataTypes.BooleanType)
                .add("s", DataTypes.StringType)
                .add("f", DataTypes.FloatType)
                .add("d", DataTypes.DoubleType)
                .add("i", DataTypes.IntegerType)
                .add("l", DataTypes.LongType)
                .add("bin", DataTypes.BinaryType);

        List<Row> first = new ArrayList<>();
        first.add(RowFactory.create(new Timestamp(TS1), true, "str-1", 1.5f, 2.5, 10, 100L,
                new byte[]{0x01, (byte) 0xff}));
        spark.createDataFrame(first, schema)
                .write()
                .mode("append")
                .jdbc(BASE_URL, created, connProps);

        // the table must have been created with the dialect's type mapping and be readable
        List<Row> rows = readTable(created).collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(new Timestamp(TS1), row.getTimestamp(0));
        assertTrue(row.getBoolean(1));
        assertEquals("str-1", row.getString(2));
        assertEquals(1.5f, row.getFloat(3), 0.0f);
        assertEquals(2.5, row.getDouble(4), 0.0);
        assertEquals(10, row.getInt(5));
        assertEquals(100L, row.getLong(6));

        // overwrite: Spark issues DROP TABLE + CREATE TABLE + INSERT
        List<Row> second = new ArrayList<>();
        second.add(RowFactory.create(new Timestamp(TS2), false, "str-2", 3.5f, 4.5, 20, 200L,
                new byte[]{0x02}));
        spark.createDataFrame(second, schema)
                .write()
                .mode("overwrite")
                .jdbc(BASE_URL, created, connProps);

        rows = readTable(created).collectAsList();
        assertEquals(1, rows.size());
        assertEquals(new Timestamp(TS2), rows.get(0).getTimestamp(0));
        assertEquals("str-2", rows.get(0).getString(2));
        assertEquals(20, rows.get(0).getInt(5));
    }

    @Test
    public void testSparkCreateTableWithTinyintSmallint() {
        // Spark binds ByteType/ShortType via setInt; taos-jdbcdriver >= 3.9.3 converts
        // the value on both the stmt2 column-bind path and the legacy row-bind path
        String created = DB + ".spark_tiny_small";

        StructType schema = new StructType()
                .add("ts", DataTypes.TimestampType)
                .add("by", DataTypes.ByteType)
                .add("sh", DataTypes.ShortType);

        List<Row> data = new ArrayList<>();
        data.add(RowFactory.create(new Timestamp(TS1), (byte) 1, (short) 2));
        spark.createDataFrame(data, schema)
                .write()
                .mode("append")
                .jdbc(BASE_URL, created, connProps);

        List<Row> rows = readTable(created).collectAsList();
        assertEquals(1, rows.size());
        assertEquals(new Timestamp(TS1), rows.get(0).getTimestamp(0));
        assertEquals(1, rows.get(0).getInt(1));
        assertEquals(2, rows.get(0).getInt(2));
    }

    @Test
    public void testAggregatePushdown() {
        // aggregate pushdown exists only in Spark's V2 JDBC path, reached through a
        // JDBCTableCatalog; V1 (format("jdbc")) never pushes aggregates in Spark 3.3.
        // The pushDownAggregate flag is a data source option, not a SQL conf in 3.3.
        spark.conf().set("spark.sql.catalog.tdengine",
                "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog");
        spark.conf().set("spark.sql.catalog.tdengine.url", BASE_URL);
        spark.conf().set("spark.sql.catalog.tdengine.user", "root");
        spark.conf().set("spark.sql.catalog.tdengine.password", "taosdata");
        spark.conf().set("spark.sql.catalog.tdengine.pushDownAggregate", "true");
        try {
            Dataset<Row> table = spark.table("tdengine." + DB + ".meters");

            Dataset<Row> agg = table.agg(
                    functions.count("*").as("c"),
                    functions.sum("voltage").as("s"),
                    functions.avg("voltage").as("a"),
                    functions.min("voltage").as("mi"),
                    functions.max("voltage").as("ma"));
            String plan = agg.queryExecution().executedPlan().toString();
            assertTrue("aggregates should be pushed to TDengine:\n" + plan,
                    plan.contains("PushedAggregates: [COUNT(*), SUM(voltage), " +
                            "AVG(voltage), MIN(voltage), MAX(voltage)]"));
            List<Row> rows = agg.collectAsList();
            assertEquals(1, rows.size());
            Row row = rows.get(0);
            assertEquals(3L, row.getLong(0));
            assertEquals(663L, row.getLong(1));
            assertEquals(221.0, row.getDouble(2), 0.0);
            assertEquals(220, row.getInt(3));
            assertEquals(222, row.getInt(4));

            Dataset<Row> grouped = table.groupBy("location").agg(functions.sum("voltage").as("s"));
            String groupPlan = grouped.queryExecution().executedPlan().toString();
            assertTrue("group by should be pushed to TDengine:\n" + groupPlan,
                    groupPlan.contains("PushedAggregates: [SUM(voltage)]") &&
                            !groupPlan.contains("PushedGroupByExpressions: []"));
            // groups: 'loc-1' -> 220, 'loc-2' -> 221, NULL -> 222
            assertEquals(3, grouped.collectAsList().size());
        } finally {
            spark.conf().unset("spark.sql.catalog.tdengine");
            spark.conf().unset("spark.sql.catalog.tdengine.url");
            spark.conf().unset("spark.sql.catalog.tdengine.user");
            spark.conf().unset("spark.sql.catalog.tdengine.password");
            spark.conf().unset("spark.sql.catalog.tdengine.pushDownAggregate");
        }
    }
}
