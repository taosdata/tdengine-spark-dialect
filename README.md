<!-- omit in toc -->
# TDengine Spark Dialect

[![build](https://github.com/taosdata/tdengine-spark-dialect/actions/workflows/build.yml/badge.svg)](https://github.com/taosdata/tdengine-spark-dialect/actions/workflows/build.yml)

English | [简体中文](./README-CN.md)

<!-- omit in toc -->
## Table of Contents

- [1. Introduction](#1-introduction)
- [2. Documentation](#2-documentation)
- [3. Prerequisites](#3-prerequisites)
- [4. Building](#4-building)
- [5. Testing](#5-testing)
  - [5.1 Test Execution](#51-test-execution)
  - [5.2 Test Case Addition](#52-test-case-addition)
  - [5.3 Performance Testing](#53-performance-testing)
- [6. Packaging](#6-packaging)
- [7. CI/CD](#7-cicd)
- [8. Submitting Issues](#8-submitting-issues)
- [9. Submitting PRs](#9-submitting-prs)
- [10. References](#10-references)
- [11. License](#11-license)

## 1. Introduction

`tdengine-spark-dialect` is the official Apache Spark JDBC dialect for TDengine. It enables Spark's JDBC data source to read from and write to TDengine with the correct data type mappings, so that TDengine-specific types such as `NCHAR` and `JSON` work out of the box.

Features:

- Supports the WebSocket connection mode of the TDengine JDBC driver (`jdbc:TAOS-WS://`), the recommended connection mode for TDengine 3.x.
- Maps TDengine column types to Spark SQL types, e.g. `NCHAR` &rarr; `StringType`, `JSON` &rarr; `StringType`.
- Maps Spark SQL types to TDengine column types when Spark creates tables, e.g. `StringType` &rarr; `VARCHAR(4096)`, `DateType` &rarr; `TIMESTAMP`.
- Quotes identifiers with backticks and provides a TDengine-compatible table-existence check.
- Compatible with Spark's V2 JDBC aggregate and `GROUP BY` pushdown (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) when the `pushDownAggregate` data source option is enabled.

## 2. Documentation

- To learn how an application introduces the TDengine JDBC driver that this dialect builds upon, please check the [Developer Guide](https://docs.tdengine.com/developer-guide/).
- For the JDBC driver reference (data types, connection parameters, FAQs), please check the [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/java/).
- For Spark's JDBC data source options (`dbtable`, `partitionColumn`, `fetchsize`, etc.), please check the [Spark SQL Data Sources documentation](https://spark.apache.org/docs/3.3.2/sql-data-sources-jdbc.html).
- This quick guide is mainly for developers who like to contribute, build, and test the Spark dialect by themselves. To learn about TDengine, you can visit the [official documentation](https://docs.tdengine.com).

### Usage

Put `tdengine-spark-dialect-<version>.jar` and `taos-jdbcdriver-<version>.jar` on the Spark classpath (e.g. `spark-submit --jars ...` or Spark's `jars/` directory).

Spark does not auto-discover JDBC dialects, so the dialect must be registered once on the driver before running JDBC queries:

```java
import com.taosdata.spark.TDengineDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

JdbcDialects.registerDialect(new TDengineDialect());
```

Reading from TDengine:

```java
Dataset<Row> df = spark.read()
        .format("jdbc")
        .option("url", "jdbc:TAOS-WS://127.0.0.1:6041/")
        .option("dbtable", "test.meters")
        .option("user", "root")
        .option("password", "taosdata")
        .load();
```

Writing to TDengine (append into an existing table):

```java
Properties props = new Properties();
props.setProperty("user", "root");
props.setProperty("password", "taosdata");

df.write()
        .mode("append")
        .jdbc("jdbc:TAOS-WS://127.0.0.1:6041/", "test.meters", props);
```

### Known Limitations

- When Spark creates a table (write with `append`/`overwrite` to a non-existing table), all fields of the DataFrame schema must be nullable, because TDengine has no `NOT NULL` constraint syntax. The first column must also be a `TIMESTAMP`, as required by TDengine.
- The `truncate` write option is not supported; TDengine has no `TRUNCATE TABLE` statement. Use `overwrite` mode instead, which drops and recreates the table.

## 3. Prerequisites

### System Requirements

- JDK >= 8
- Maven >= 3.6
- Apache Spark 3.3.x at runtime (this project builds against Spark 3.3.2 / Scala 2.13)
- TDengine 3.x server
- `com.taosdata.jdbc:taos-jdbcdriver` on the Spark classpath at runtime

### Installing Build Tools

**Ubuntu/Debian:**

```bash
sudo apt-get update && sudo apt-get install -y openjdk-8-jdk maven
```

**CentOS/RHEL:**

```bash
sudo yum install -y java-1.8.0-openjdk-devel maven
```

### Local Test Environment

- TDengine has been deployed locally. For specific steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Please make sure taosd and taosAdapter have been started; the integration tests connect via WebSocket on port 6041.

## 4. Building

```bash
git clone https://github.com/taosdata/tdengine-spark-dialect.git
cd tdengine-spark-dialect
mvn clean package -Dmaven.test.skip=true
```

Output: `target/tdengine-spark-dialect-<version>.jar`

## 5. Testing

### 5.1 Test Execution

Execute `mvn test` in the project directory to run the tests.

- Unit tests cover the dialect class itself (URL handling, identifier quoting, type mappings) and need no database.
- Integration tests start a local-mode SparkSession, then read from and write to the local TDengine server through the WebSocket connection (`jdbc:TAOS-WS://127.0.0.1:6041`). They are skipped automatically when the server is not reachable.

After running the tests, a result similar to the following will be printed eventually. If all test cases pass, both Failures and Errors will be 0.

```
[INFO] Results:
[INFO]
[INFO] Tests run: 14, Failures: 0, Errors: 0, Skipped: 0
```

```bash
# run the full test suite
mvn test
```

- Make sure `taosd` and `taosAdapter` are running before executing the integration tests.
- If you only need to verify packaging, use `mvn clean package -Dmaven.test.skip=true` from the build step above.

### 5.2 Test Case Addition

All tests are located in the `src/test/java/com/taosdata/spark` directory of the project. Unit tests are in `TDengineDialectTest`; integration tests are in `TDengineDialectIntegrationTest` (end-to-end read/write) and `TDengineGeneratedSqlTest` (the SQL Spark generates: table-existence probe, filter pushdown, CREATE/DROP TABLE, INSERT). You can add new test files or add test cases in existing test files.

The test cases use the JUnit 4 framework. For the integration tests, a dedicated database (`spark_dialect_test`) with a super table of all common TDengine types is created in the `@BeforeClass` method, and the database is dropped in the `@AfterClass` method.

### 5.3 Performance Testing

Performance testing is in progress.

## 6. Packaging

```bash
mvn clean package -Dmaven.test.skip=true
# Output: target/tdengine-spark-dialect-<version>.jar
```

## 7. CI/CD

- [Build Workflow](https://github.com/taosdata/tdengine-spark-dialect/actions/workflows/build.yml): runs `mvn clean verify` on JDK 8 against a TDengine server started from the official Docker image.

## 8. Submitting Issues

We welcome the submission of [GitHub Issue](https://github.com/taosdata/tdengine-spark-dialect/issues/new). When submitting, please provide the following information:

- Problem description, whether it always occurs, and it's best to include a detailed call stack.
- Spark dialect version and Spark version.
- JDBC driver version.
- TDengine server version.

## 9. Submitting PRs

We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project, refer to ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
1. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`). Do not modify the main branch directly.
1. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
1. Push the changes to the remote branch (`git push origin my_branch`).
1. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
1. After submitting the PR, you can find your PR through the [Pull Request](https://github.com/taosdata/tdengine-spark-dialect/pulls) page and check whether the CI for your PR has passed.

## 10. References

- [TDengine Official Website](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)
- [TDengine JDBC Connector GitHub](https://github.com/taosdata/taos-connector-jdbc)
- [Apache Spark JDBC Data Source](https://spark.apache.org/docs/3.3.2/sql-data-sources-jdbc.html)

## 11. License

[MIT License](./LICENSE)
