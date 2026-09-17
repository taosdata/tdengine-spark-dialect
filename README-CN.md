<!-- omit in toc -->
# TDengine Spark 方言

[![build](https://github.com/taosdata/tdengine-spark-dialect/actions/workflows/build.yml/badge.svg)](https://github.com/taosdata/tdengine-spark-dialect/actions/workflows/build.yml)

[English](./README.md) | 简体中文

<!-- omit in toc -->
## 目录

- [1. 简介](#1-简介)
- [2. 文档](#2-文档)
- [3. 前置条件](#3-前置条件)
- [4. 构建](#4-构建)
- [5. 测试](#5-测试)
  - [5.1 执行测试](#51-执行测试)
  - [5.2 添加测试用例](#52-添加测试用例)
  - [5.3 性能测试](#53-性能测试)
- [6. 打包](#6-打包)
- [7. CI/CD](#7-cicd)
- [8. 提交 Issue](#8-提交-issue)
- [9. 提交 PR](#9-提交-pr)
- [10. 引用](#10-引用)
- [11. 许可证](#11-许可证)

## 1. 简介

`tdengine-spark-dialect` 是 TDengine 官方的 Apache Spark JDBC 方言。它让 Spark 的 JDBC 数据源能够以正确的数据类型映射读写 TDengine，使 `NCHAR`、`JSON` 等 TDengine 特有类型开箱即用。

功能特性：

- 支持 TDengine JDBC 驱动的 WebSocket 连接方式（`jdbc:TAOS-WS://`），即 TDengine 3.x 推荐的连接方式。
- 将 TDengine 列类型映射为 Spark SQL 类型，例如 `NCHAR` &rarr; `StringType`、`JSON` &rarr; `StringType`。
- 在 Spark 建表时将 Spark SQL 类型映射为 TDengine 列类型，例如 `StringType` &rarr; `VARCHAR(4096)`、`DateType` &rarr; `TIMESTAMP`。
- 使用反引号引用标识符，并提供适配 TDengine 的表存在性检查。
- 兼容 Spark V2 JDBC 的聚合与 `GROUP BY` 下推（`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`），需开启 `pushDownAggregate` 数据源选项。

## 2. 文档

- 了解应用程序如何引入本方言语依赖的 TDengine JDBC 驱动，请参考[开发指南](https://docs.tdengine.com/developer-guide/)。
- JDBC 驱动的参考信息（数据类型、连接参数、常见问题），请参考[参考手册](https://docs.tdengine.com/tdengine-reference/client-libraries/java/)。
- Spark JDBC 数据源的选项说明（`dbtable`、`partitionColumn`、`fetchsize` 等），请参考 [Spark SQL 数据源文档](https://spark.apache.org/docs/3.3.2/sql-data-sources-jdbc.html)。
- 本快速指南主要面向希望自行贡献、构建和测试 Spark 方言的开发者。如需了解 TDengine，请访问[官方文档](https://docs.tdengine.com)。

### 使用方法

将 `tdengine-spark-dialect-<version>.jar` 和 `taos-jdbcdriver-<version>.jar` 放到 Spark classpath 上（例如 `spark-submit --jars ...` 或 Spark 的 `jars/` 目录）。

Spark 不会自动发现 JDBC 方言，需要在 driver 端执行 JDBC 查询前先注册一次：

```java
import com.taosdata.spark.TDengineDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

JdbcDialects.registerDialect(new TDengineDialect());
```

从 TDengine 读取数据：

```java
Dataset<Row> df = spark.read()
        .format("jdbc")
        .option("url", "jdbc:TAOS-WS://127.0.0.1:6041/")
        .option("dbtable", "test.meters")
        .option("user", "root")
        .option("password", "taosdata")
        .load();
```

向 TDengine 写入数据（追加到已有表）：

```java
Properties props = new Properties();
props.setProperty("user", "root");
props.setProperty("password", "taosdata");

df.write()
        .mode("append")
        .jdbc("jdbc:TAOS-WS://127.0.0.1:6041/", "test.meters", props);
```

### 已知限制

- 由 Spark 建表时（以 `append`/`overwrite` 模式写入不存在的表），DataFrame schema 的所有字段必须可空，因为 TDengine 没有 `NOT NULL` 约束语法；同时按 TDengine 要求，第一列必须是 `TIMESTAMP` 类型。
- 不支持 `truncate` 写入选项，TDengine 没有 `TRUNCATE TABLE` 语句。需要整表覆盖时请使用 `overwrite` 模式（先删表再重建）。
- 向 `TINYINT`/`SMALLINT` 列写入 Spark `ByteType`/`ShortType` 数据要求 TDengine 服务端 >= 3.4.1.13。Spark 对这两种类型一律调用 `setInt` 绑定，只有服务端 >= 3.4.1.13 时驱动使用的 stmt2 绑定路径才会做数值转换；更早版本走驱动 legacy 行绑定路径，会以 `ClassCastException` 失败。其余类型不受影响。

## 3. 前置条件

### 系统要求

- JDK >= 8
- Maven >= 3.6
- 运行时需要 Apache Spark 3.3.x（本项目基于 Spark 3.3.2 / Scala 2.13 构建）
- TDengine 3.x 服务端
- 运行时 Spark classpath 上需要有 `com.taosdata.jdbc:taos-jdbcdriver`

### 安装构建工具

**Ubuntu/Debian：**

```bash
sudo apt-get update && sudo apt-get install -y openjdk-8-jdk maven
```

**CentOS/RHEL：**

```bash
sudo yum install -y java-1.8.0-openjdk-devel maven
```

### 本地测试环境

- 已在本地部署 TDengine。具体步骤请参考[部署 TDengine](https://docs.tdengine.com/get-started/deploy-from-package/)。请确保 taosd 和 taosAdapter 已经启动；集成测试通过 6041 端口的 WebSocket 连接。

## 4. 构建

```bash
git clone https://github.com/taosdata/tdengine-spark-dialect.git
cd tdengine-spark-dialect
mvn clean package -Dmaven.test.skip=true
```

输出：`target/tdengine-spark-dialect-<version>.jar`

## 5. 测试

### 5.1 执行测试

在项目目录下执行 `mvn test` 运行测试。

- 单元测试覆盖方言类本身（URL 匹配、标识符引用、类型映射），不需要数据库。
- 集成测试会启动一个 local 模式的 SparkSession，通过 WebSocket 连接（`jdbc:TAOS-WS://127.0.0.1:6041`）对本地 TDengine 服务端进行真实的读写测试。当服务端不可达时会自动跳过。如需连接其他 TDengine 地址，可用 `-Dtdengine.ws.url=...` 覆盖：

```bash
mvn test -Dtdengine.ws.url=jdbc:TAOS-WS://192.168.1.100:6041/
```

测试运行结束后，会打印类似如下结果。全部用例通过时，Failures 和 Errors 均为 0。

```
[INFO] Results:
[INFO]
[INFO] Tests run: 15, Failures: 0, Errors: 0, Skipped: 0
```

当 TDengine 服务端版本低于 3.4.1.13 时，实际运行的用例会略少——`ByteType`/`ShortType` 写入相关用例会被跳过（见[已知限制](#已知限制)）。

```bash
# 运行完整测试套件
mvn test
```

- 执行集成测试前，请确保 `taosd` 和 `taosAdapter` 已启动。
- 如果只需要验证打包，请使用上面构建步骤中的 `mvn clean package -Dmaven.test.skip=true`。

### 5.2 添加测试用例

所有测试位于项目的 `src/test/java/com/taosdata/spark` 目录。单元测试在 `TDengineDialectTest` 中；集成测试在 `TDengineDialectIntegrationTest`（端到端读写）和 `TDengineGeneratedSqlTest`（Spark 生成的各类 SQL：表存在性探测、过滤下推、CREATE/DROP TABLE、INSERT）中。可以添加新的测试文件，或在现有测试文件中添加测试用例。

测试用例使用 JUnit 4 框架。集成测试在 `@BeforeClass` 方法中创建专用数据库（`spark_dialect_test`）以及一张覆盖 TDengine 常用类型的超级表，并在 `@AfterClass` 方法中删除该数据库。

### 5.3 性能测试

性能测试正在进行中。

## 6. 打包

```bash
mvn clean package -Dmaven.test.skip=true
# 输出：target/tdengine-spark-dialect-<version>.jar
```

## 7. CI/CD

- [Build Workflow](https://github.com/taosdata/tdengine-spark-dialect/actions/workflows/build.yml)：在 JDK 8 上执行 `mvn clean verify`，TDengine 服务端由官方 Docker 镜像启动。

## 8. 提交 Issue

我们欢迎提交 [GitHub Issue](https://github.com/taosdata/tdengine-spark-dialect/issues/new)。提交时请提供以下信息：

- 问题描述、是否必现，最好附上详细的调用栈。
- Spark 方言版本和 Spark 版本。
- JDBC 驱动版本。
- TDengine 服务端版本。

## 9. 提交 PR

我们欢迎开发者为本项目做贡献。提交 PR 时请遵循以下步骤：

1. Fork 本项目，参考（[如何 fork 仓库](https://docs.github.com/en/get-started/quickstart/fork-a-repo)）。
1. 从 main 分支创建一个有明确含义的新分支（`git checkout -b my_branch`），不要直接修改 main 分支。
1. 修改代码，确保所有单元测试通过，并为改动添加新的单元测试。
1. 推送改动到远程分支（`git push origin my_branch`）。
1. 在 GitHub 上创建 Pull Request（[如何创建 PR](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)）。
1. 提交 PR 后，可以在 [Pull Request](https://github.com/taosdata/tdengine-spark-dialect/pulls) 页面找到你的 PR，并查看 CI 是否通过。

## 10. 引用

- [TDengine 官网](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)
- [TDengine JDBC 连接器 GitHub](https://github.com/taosdata/taos-connector-jdbc)
- [Apache Spark JDBC 数据源](https://spark.apache.org/docs/3.3.2/sql-data-sources-jdbc.html)

## 11. 许可证

[MIT License](./LICENSE)
