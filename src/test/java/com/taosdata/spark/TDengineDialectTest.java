package com.taosdata.spark;

import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.junit.Test;
import scala.Option;

import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TDengineDialectTest {

    private final TDengineDialect dialect = new TDengineDialect();

    @Test
    public void testCanHandle() {
        assertTrue(dialect.canHandle("jdbc:TAOS://127.0.0.1:6030/test"));
        assertTrue(dialect.canHandle("jdbc:TAOS-WS://127.0.0.1:6041/test"));
        assertTrue(dialect.canHandle("jdbc:TAOS-RS://127.0.0.1:6041/test"));
        assertFalse(dialect.canHandle("jdbc:mysql://127.0.0.1:3306/test"));
        assertFalse(dialect.canHandle("jdbc:postgresql://127.0.0.1:5432/test"));
        assertFalse(dialect.canHandle(null));
    }

    @Test
    public void testQuoteIdentifier() {
        assertEquals("`ts`", dialect.quoteIdentifier("ts"));
    }

    @Test
    public void testGetTableExistsQuery() {
        assertEquals("SELECT * FROM test.meters LIMIT 0", dialect.getTableExistsQuery("test.meters"));
    }

    @Test
    public void testGetCatalystType() {
        MetadataBuilder md = new MetadataBuilder();

        // TDengine-specific types that Spark's default mapping cannot handle
        assertEquals(Option.apply(DataTypes.StringType),
                dialect.getCatalystType(Types.NCHAR, "NCHAR", 64, md));
        assertEquals(Option.apply(DataTypes.StringType),
                dialect.getCatalystType(Types.NVARCHAR, "NVARCHAR", 64, md));
        assertEquals(Option.apply(DataTypes.StringType),
                dialect.getCatalystType(Types.LONGNVARCHAR, "LONGNVARCHAR", 64, md));
        assertEquals(Option.apply(DataTypes.StringType),
                dialect.getCatalystType(Types.OTHER, "JSON", 0, md));

        // types covered by Spark's default mapping: the dialect must abstain
        assertEquals(Option.empty(), dialect.getCatalystType(Types.BOOLEAN, "BOOL", 0, md));
        assertEquals(Option.empty(), dialect.getCatalystType(Types.TINYINT, "TINYINT", 0, md));
        assertEquals(Option.empty(), dialect.getCatalystType(Types.SMALLINT, "SMALLINT", 0, md));
        assertEquals(Option.empty(), dialect.getCatalystType(Types.INTEGER, "INT", 0, md));
        assertEquals(Option.empty(), dialect.getCatalystType(Types.BIGINT, "BIGINT", 0, md));
        assertEquals(Option.empty(), dialect.getCatalystType(Types.FLOAT, "FLOAT", 0, md));
        assertEquals(Option.empty(), dialect.getCatalystType(Types.DOUBLE, "DOUBLE", 0, md));
        assertEquals(Option.empty(), dialect.getCatalystType(Types.VARCHAR, "VARCHAR", 64, md));
        assertEquals(Option.empty(), dialect.getCatalystType(Types.TIMESTAMP, "TIMESTAMP", 0, md));
        assertEquals(Option.empty(), dialect.getCatalystType(Types.VARBINARY, "VARBINARY", 64, md));
        assertEquals(Option.empty(), dialect.getCatalystType(Types.BINARY, "GEOMETRY", 0, md));
    }

    @Test
    public void testGetJDBCType() {
        assertEquals(Option.apply(new JdbcType("VARCHAR(4096)", Types.VARCHAR)),
                dialect.getJDBCType(DataTypes.StringType));
        assertEquals(Option.apply(new JdbcType("FLOAT", Types.FLOAT)),
                dialect.getJDBCType(DataTypes.FloatType));
        assertEquals(Option.apply(new JdbcType("TIMESTAMP", Types.TIMESTAMP)),
                dialect.getJDBCType(DataTypes.DateType));
        assertEquals(Option.apply(new JdbcType("VARBINARY(4096)", Types.VARBINARY)),
                dialect.getJDBCType(DataTypes.BinaryType));

        // types with working Spark defaults: the dialect must abstain
        assertEquals(Option.empty(), dialect.getJDBCType(DataTypes.BooleanType));
        assertEquals(Option.empty(), dialect.getJDBCType(DataTypes.ByteType));
        assertEquals(Option.empty(), dialect.getJDBCType(DataTypes.ShortType));
        assertEquals(Option.empty(), dialect.getJDBCType(DataTypes.IntegerType));
        assertEquals(Option.empty(), dialect.getJDBCType(DataTypes.LongType));
        assertEquals(Option.empty(), dialect.getJDBCType(DataTypes.DoubleType));
        assertEquals(Option.empty(), dialect.getJDBCType(DataTypes.TimestampType));
    }
}
