package com.taosdata.spark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Detects whether the connected server forces the JDBC driver onto its legacy
 * row-bind path. The stmt2 column-bind path, which converts mismatched bind
 * values (Spark always binds ByteType/ShortType via setInt), requires
 * server >= 3.4.1.13; see taos-jdbcdriver VersionUtil.supportStmt2BindExec.
 */
final class TestServerInfo {

    private TestServerInfo() {
    }

    static boolean isLegacyBindPath(Connection conn) throws SQLException {
        String version;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT server_version()")) {
            rs.next();
            version = rs.getString(1).trim();
        }
        return compareVersions(version, "3.4.1.13") < 0;
    }

    private static int compareVersions(String a, String b) {
        String[] pa = a.split("\\.");
        String[] pb = b.split("\\.");
        int len = Math.max(pa.length, pb.length);
        for (int i = 0; i < len; i++) {
            int na = i < pa.length ? numericPrefix(pa[i]) : 0;
            int nb = i < pb.length ? numericPrefix(pb[i]) : 0;
            if (na != nb) {
                return na < nb ? -1 : 1;
            }
        }
        return 0;
    }

    private static int numericPrefix(String part) {
        int end = 0;
        while (end < part.length() && Character.isDigit(part.charAt(end))) {
            end++;
        }
        return end == 0 ? 0 : Integer.parseInt(part.substring(0, end));
    }
}
