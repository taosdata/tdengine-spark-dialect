package com.taosdata.spark;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import scala.Option;

import java.sql.Types;

/**
 * Spark JDBC dialect for TDengine.
 *
 * <p>Handles only the WebSocket URL prefix of the TDengine JDBC driver
 * ({@code jdbc:TAOS-WS://}), the recommended connection mode for TDengine 3.x.
 *
 * <p>Spark does not auto-discover JDBC dialects; register the dialect once on the driver
 * before running JDBC queries:
 * <pre>{@code JdbcDialects.registerDialect(new TDengineDialect());}</pre>
 */
public class TDengineDialect extends JdbcDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean canHandle(String url) {
        return url != null && url.startsWith("jdbc:TAOS-WS://");
    }

    @Override
    public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
        switch (sqlType) {
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                // TDengine NCHAR columns are reported with the N* type codes,
                // which Spark's default mapping does not cover
                return Option.apply(DataTypes.StringType);
            case Types.OTHER:
                // TDengine JSON tags are reported as Types.OTHER; expose them as plain strings
                return Option.apply(DataTypes.StringType);
            default:
                // BOOL, TINYINT..BIGINT, the unsigned integer families, FLOAT, DOUBLE, VARCHAR,
                // TIMESTAMP, VARBINARY, GEOMETRY, DECIMAL and BLOB are covered by Spark's
                // default mapping, so the dialect abstains
                return Option.empty();
        }
    }

    @Override
    public Option<JdbcType> getJDBCType(DataType dt) {
        if (DataTypes.StringType.sameType(dt)) {
            // Spark's default is TEXT, which TDengine does not support;
            // VARCHAR requires an explicit length in TDengine
            return Option.apply(new JdbcType("VARCHAR(4096)", Types.VARCHAR));
        }
        if (DataTypes.FloatType.sameType(dt)) {
            // Spark's default is REAL, which TDengine does not support
            return Option.apply(new JdbcType("FLOAT", Types.FLOAT));
        }
        if (DataTypes.DateType.sameType(dt)) {
            // TDengine has no DATE type; store dates as timestamps
            return Option.apply(new JdbcType("TIMESTAMP", Types.TIMESTAMP));
        }
        if (DataTypes.BinaryType.sameType(dt)) {
            // VARBINARY is supported by every TDengine 3.x server, while BLOB is only
            // available on newer ones
            return Option.apply(new JdbcType("VARBINARY(4096)", Types.VARBINARY));
        }
        return Option.empty();
    }

    @Override
    public String quoteIdentifier(String colName) {
        return "`" + colName + "`";
    }

    @Override
    public String getTableExistsQuery(String table) {
        // Spark decides "table exists" solely on whether this query executes without error,
        // so a plain LIMIT 0 probe is the reliable check: it succeeds for normal tables,
        // super tables and views, and fails when the table does not exist.
        return "SELECT * FROM " + table + " LIMIT 0";
    }
}
