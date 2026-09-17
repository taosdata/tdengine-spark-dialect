package com.taosdata.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests against a locally running TDengine server (taosd and taosAdapter).
 * The whole class is skipped automatically when the server is not reachable.
 */
public class TDengineDialectIntegrationTest {

    private static final String BASE_URL = "jdbc:TAOS-WS://127.0.0.1:6041/";
    private static final String DB = "spark_dialect_test";
    private static final String TABLE = DB + ".d1001";
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
            stmt.execute("CREATE STABLE IF NOT EXISTS " + DB + ".meters (" +
                    "ts TIMESTAMP, current FLOAT, voltage INT, phase DOUBLE, " +
                    "location NCHAR(64), note VARCHAR(100), active BOOL, " +
                    "tiny TINYINT, small SMALLINT, big BIGINT, vb VARBINARY(64)) " +
                    "TAGS (group_id INT, area NCHAR(32))");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " USING " + DB + ".meters TAGS (1, 'beijing')");
            stmt.execute("INSERT INTO " + TABLE + " VALUES " +
                    "(" + TS1 + ", 1.5, 220, 3.14, 'loc-1', 'note-1', true, 1, 10, 1000, '\\x0102ab'), " +
                    "(" + TS2 + ", 2.5, 221, 6.28, 'loc-2', 'note-2', false, 2, 20, 2000, '\\x0304cd')");
            // super table with INT and NCHAR tags, two child tables
            stmt.execute("CREATE STABLE IF NOT EXISTS " + DB + ".weather (" +
                    "ts TIMESTAMP, temperature DOUBLE, humidity INT) TAGS (city NCHAR(32), note_id INT)");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + DB + ".w1 USING " + DB + ".weather TAGS ('beijing', 1)");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + DB + ".w2 USING " + DB + ".weather TAGS ('shanghai', 2)");
            stmt.execute("INSERT INTO " + DB + ".w1 VALUES (" + TS1 + ", 25.5, 40)");
            stmt.execute("INSERT INTO " + DB + ".w2 VALUES (" + TS2 + ", 30.5, 65)");
            // a JSON tag must be the only tag of its super table
            stmt.execute("CREATE STABLE IF NOT EXISTS " + DB + ".devices (" +
                    "ts TIMESTAMP, v DOUBLE) TAGS (info JSON)");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + DB + ".dev1 USING " + DB + ".devices " +
                    "TAGS ('{\"site\":\"beijing\",\"floor\":3}')");
            stmt.execute("INSERT INTO " + DB + ".dev1 VALUES (" + TS1 + ", 1.1)");
        } finally {
            conn.close();
        }

        // Spark does not auto-discover JDBC dialects, so registration is required
        JdbcDialects.registerDialect(TestDialect.INSTANCE);

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("TDengineDialectIntegrationTest")
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
    public void testDialectRegistration() {
        assertTrue(JdbcDialects.get(BASE_URL) instanceof TDengineDialect);
    }

    @Test
    public void testReadSuperTableWithTags() {
        // reading a super table returns the rows of all child tables plus the tag columns
        Dataset<Row> df = readTable(DB + ".weather");

        StructType schema = df.schema();
        assertEquals(DataTypes.TimestampType, schema.apply("ts").dataType());
        assertEquals(DataTypes.DoubleType, schema.apply("temperature").dataType());
        assertEquals(DataTypes.IntegerType, schema.apply("humidity").dataType());
        // the NCHAR tag must come back as StringType; without the dialect Spark cannot map it
        assertEquals(DataTypes.StringType, schema.apply("city").dataType());
        assertEquals(DataTypes.IntegerType, schema.apply("note_id").dataType());

        List<Row> rows = df.orderBy("ts").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(new Timestamp(TS1), rows.get(0).getTimestamp(0));
        assertEquals(25.5, rows.get(0).getDouble(1), 0.0);
        assertEquals(40, rows.get(0).getInt(2));
        assertEquals("beijing", rows.get(0).getString(3));
        assertEquals(1, rows.get(0).getInt(4));
        assertEquals("shanghai", rows.get(1).getString(3));
        assertEquals(2, rows.get(1).getInt(4));
    }

    @Test
    public void testReadJsonTag() {
        Dataset<Row> df = readTable(DB + ".devices");

        // the driver reports JSON tags as Types.OTHER, the dialect maps them to StringType
        assertEquals(DataTypes.StringType, df.schema().apply("info").dataType());

        List<Row> rows = df.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(1.1, rows.get(0).getDouble(1), 0.0);
        String info = rows.get(0).getString(2);
        assertTrue(info.contains("beijing"));
        assertTrue(info.contains("\"floor\":3") || info.contains("\"floor\": 3"));
    }

    @Test
    public void testReadFromTDengine() {
        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", BASE_URL)
                .option("dbtable", TABLE)
                .option("user", "root")
                .option("password", "taosdata")
                .load();

        StructType schema = df.schema();
        assertEquals(DataTypes.TimestampType, schema.apply("ts").dataType());
        assertEquals(DataTypes.FloatType, schema.apply("current").dataType());
        assertEquals(DataTypes.IntegerType, schema.apply("voltage").dataType());
        assertEquals(DataTypes.DoubleType, schema.apply("phase").dataType());
        // NCHAR/VARCHAR must come back as StringType; without the dialect, Spark's default
        // mapping cannot handle NCHAR at all
        assertEquals(DataTypes.StringType, schema.apply("location").dataType());
        assertEquals(DataTypes.StringType, schema.apply("note").dataType());
        assertEquals(DataTypes.BooleanType, schema.apply("active").dataType());
        // Spark's default mapping widens TINYINT and SMALLINT to IntegerType
        assertEquals(DataTypes.IntegerType, schema.apply("tiny").dataType());
        assertEquals(DataTypes.IntegerType, schema.apply("small").dataType());
        assertEquals(DataTypes.LongType, schema.apply("big").dataType());
        assertEquals(DataTypes.BinaryType, schema.apply("vb").dataType());

        // filter out rows written by testWriteToTDengine so the two tests stay independent
        List<Row> rows = df.filter("voltage < 222").orderBy("ts").collectAsList();
        assertEquals(2, rows.size());

        Row r1 = rows.get(0);
        assertEquals(new Timestamp(TS1), r1.getTimestamp(0));
        assertEquals(1.5f, r1.getFloat(1), 0.0f);
        assertEquals(220, r1.getInt(2));
        assertEquals(3.14, r1.getDouble(3), 0.0);
        assertEquals("loc-1", r1.getString(4));
        assertEquals("note-1", r1.getString(5));
        assertTrue(r1.getBoolean(6));
        assertEquals(1, r1.getInt(7));
        assertEquals(10, r1.getInt(8));
        assertEquals(1000L, r1.getLong(9));
        assertArrayEquals(new byte[]{0x01, 0x02, (byte) 0xab}, (byte[]) r1.getAs("vb"));

        Row r2 = rows.get(1);
        assertEquals(new Timestamp(TS2), r2.getTimestamp(0));
        assertEquals(2.5f, r2.getFloat(1), 0.0f);
        assertEquals("loc-2", r2.getString(4));
        assertTrue(!r2.getBoolean(6));
        assertArrayEquals(new byte[]{0x03, 0x04, (byte) 0xcd}, (byte[]) r2.getAs("vb"));
    }

    @Test
    public void testWriteToTDengine() {
        StructType schema = new StructType()
                .add("ts", DataTypes.TimestampType)
                .add("current", DataTypes.FloatType)
                .add("voltage", DataTypes.IntegerType)
                .add("phase", DataTypes.DoubleType)
                .add("location", DataTypes.StringType)
                .add("note", DataTypes.StringType)
                .add("active", DataTypes.BooleanType)
                .add("tiny", DataTypes.ByteType)
                .add("small", DataTypes.ShortType)
                .add("big", DataTypes.LongType)
                .add("vb", DataTypes.BinaryType);

        List<Row> data = new ArrayList<>();
        data.add(RowFactory.create(
                new Timestamp(TS3), 3.5f, 222, 9.42, "loc-3", "note-3", true,
                (byte) 3, (short) 30, 3000L, new byte[]{0x05, 0x06, (byte) 0xef}));
        Dataset<Row> toWrite = spark.createDataFrame(data, schema);

        toWrite.write()
                .mode("append")
                .jdbc(BASE_URL, TABLE, connProps);

        List<Row> rows = spark.read()
                .format("jdbc")
                .option("url", BASE_URL)
                .option("dbtable", TABLE)
                .option("user", "root")
                .option("password", "taosdata")
                .load()
                .filter("voltage = 222")
                .collectAsList();
        assertEquals(1, rows.size());

        Row row = rows.get(0);
        assertEquals(new Timestamp(TS3), row.getTimestamp(0));
        assertEquals(3.5f, row.getFloat(1), 0.0f);
        assertEquals(222, row.getInt(2));
        assertEquals(9.42, row.getDouble(3), 0.0);
        assertEquals("loc-3", row.getString(4));
        assertEquals("note-3", row.getString(5));
        assertTrue(row.getBoolean(6));
        assertEquals(3, row.getInt(7));
        assertEquals(30, row.getInt(8));
        assertEquals(3000L, row.getLong(9));
        assertArrayEquals(new byte[]{0x05, 0x06, (byte) 0xef}, (byte[]) row.getAs("vb"));
    }
}
