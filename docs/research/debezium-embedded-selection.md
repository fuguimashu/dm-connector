# Debezium 版本与嵌入式引擎选型调研

- 日期：2026-07-27
- 关联 Issue：[fuguimashu/dm-connector#4](https://github.com/fuguimashu/dm-connector/issues/4)
- 调研范围：我们基于哪个 Debezium 版本构建、复用到什么深度、依赖树能压到多轻。
- 方法：以 debezium.io 官方文档 / debezium GitHub 源码与 POM / Maven Central 为准；每条结论附来源链接。无法从一手来源确认的，统一进「未证实事项」。

---

## 结论

### 1. 版本：基于 Debezium **3.6.x（当前 latest stable，3.6.0.Final 发布于 2026-07-01）**，运行时基线 **JDK 17**，构建时 **JDK 21 + Maven 3.9.8+**

- 3.6 是当前 latest stable，3.5 / 2.7 是站点上另外两个仍标为 stable 的系列；1.9 早已停更（最后一版 1.9.8.Final，2023-12-15）。见 [Debezium Releases Overview](https://debezium.io/releases/)。
- 3.x 连接器运行时基线是 Java 17，Debezium Server / Operator / Outbox / Quarkus 扩展才要 21；从源码构建要 JDK 21。见 [Debezium 3.0.0.Final 发布公告](https://debezium.io/blog/2024/10/02/debezium-3-0-final-released/) 与 [Releases Overview 的 Tested Versions 表](https://debezium.io/releases/)。
- **不要跟随国内参考实现停留在 1.9**。1.9 的问题不只是"旧"：它对应 Kafka 3.2.0、`DatabaseHistory` 老命名、老配置键（`database.history.*` / `database.server.name`），并且 `DebeziumEngine` 少了 header format 与 builderFactory 选择能力（见下文第 1、2 节）。做一个全新的达梦连接器，没有任何存量兼容包袱要背，直接上 3.x 是最省事的。
- 若达梦生产环境被 JDK 8 锁死，则 3.x 与 2.x 都不可用（2.x 基线 JDK 11），只能退回 1.9（`maven.compiler.target=1.8`）。这是**唯一**应该考虑 1.9 的理由，需要先确认目标环境 JDK。

### 2. 复用深度：**深度复用 `debezium-connector-common`（连接器框架），浅度复用 `debezium-embedded`（引擎）**

推荐架构与 Debezium 官方外置连接器仓库（如 [`debezium-connector-yashandb`](https://github.com/debezium/debezium-connector-yashandb)，崖山数据库，同为国产库）完全一致：

- 我们的 `dm-connector` 以 **compile** 依赖 `debezium-connector-common`（+ `debezium-config` / `debezium-util`），**provided** 依赖 `connect-api` / `slf4j-api`；
- `debezium-embedded` 只在**集成测试**与**独立进程外壳**里用，作为可选依赖，不进核心库的 compile 范围；
- 对外暴露的门面是 `io.debezium.engine.DebeziumEngine`（`debezium-api`，31 KB，是 Debezium 唯一做 API 兼容性校验的模块）。

理由：`debezium-connector-common` 给的是"写一个关系型 CDC 连接器所需的全部脚手架"（快照/流式事件源、EventDispatcher、Offset/Partition 抽象、RelationalDatabaseSchema、Envelope、SchemaHistory……），这是最大的复用价值；而 `debezium-embedded` 只是一个"跑单连接器的 Worker"，它是把整个 Kafka Connect runtime 拖进来的元凶。

### 3. 最小依赖树：**约 15 MB，其中 kafka-clients 一个就占 ~10 MB；`connect-api` 与 `connect-runtime` 都无法去掉**

- `connect-api` **不可去除**：`SourceRecord` / `SourceTask` / `Schema` / `Struct` / `Converter` 全在里面，Debezium 的 `Envelope`、`BaseSourceTask` 直接引用。
- `connect-runtime` **不可去除（只要用 `debezium-embedded`）**：`AsyncEmbeddedEngine` 直接 import `AbstractHerder`、`WorkerConfig`、`OffsetBackingStore`、`OffsetStorageWriter`、`ConnectorTaskId` 等 runtime 类。Debezium 官方设计文档 [DDD-7](https://github.com/debezium/debezium-design-documents/blob/main/DDD-7.md) 明确把"移除 Kafka Connect API 依赖"列为 **Non-goal**。
- `kafka-clients` **不可去除**：`connect-api` 对它是 compile 依赖。
- **可裁剪**的是 `connect-runtime` 的一大票 `runtime` scope 传递依赖（Jetty / Jersey / jose4j / classgraph / maven-artifact / swagger-annotations / jaxb / activation），因为嵌入式模式不启动 Connect REST server。这块能省掉数 MB。
- 若要突破 15 MB 下限，唯一路径是**不用 `debezium-embedded`，自己写一个百来行的 mini engine 驱动 `SourceTask`**（见第 3 节 方案 C）。这能去掉 `connect-runtime`（817 KB）及其全部 runtime 传递依赖，但仍去不掉 `connect-api` + `kafka-clients`。

### 4. 许可证：Apache-2.0，**可以闭源/商业分发**，但要保留 NOTICE/LICENSE 与变更声明

见第 6 节。

---

## 1. 嵌入式引擎的现状（API 形态与演进）

### 1.1 `DebeziumEngine` 接口所在位置

`io.debezium.engine.DebeziumEngine`，模块 `io.debezium:debezium-api`。这是 Debezium 唯一开启 revapi API 兼容性检查的模块（见第 5.3 节）。

- 3.x 源码：[debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java](https://github.com/debezium/debezium/blob/main/debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java)
- 官方文档：[Debezium Engine](https://debezium.io/documentation/reference/stable/development/engine.html)

### 1.2 两种实现与默认值的演进（关键）

官方文档原文（[engine.html](https://debezium.io/documentation/reference/stable/development/engine.html)）：

> Beginning with the 2.6.0 release, Debezium provides two implementations of the DebeziumEngine interface.
> The older EmbeddedEngine implementation runs a single connector that uses only one task. …
> **EmbeddedEngine is the default implementation in Debezium release 3.1.0.Final and older.**
> **Starting Debezium release 3.2.0.Alpha1, the default implementation is AsyncEmbeddedEngine and EmbeddedEngine implementation is not available anymore.**

代码层面已核实：

| 版本 | `io.debezium.embedded` 包内容 | 默认 BuilderFactory |
| --- | --- | --- |
| 1.9.8.Final | `EmbeddedEngine` + `ConvertingEngineBuilder` | 通过 `ServiceLoader` 取第一个实现 |
| 2.6.0.Final | `EmbeddedEngine` + `ConvertingEngineBuilder` + 新增 `async/` 包 | `ServiceLoader` 取第一个实现（[源码](https://github.com/debezium/debezium/blob/v2.6.0.Final/debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java)） |
| [3.1.0.Final](https://github.com/debezium/debezium/tree/v3.1.0.Final/debezium-embedded/src/main/java/io/debezium/embedded) | `EmbeddedEngine`、`ConvertingEngineBuilder`、`ConvertingEngineBuilderFactory` + `async/` | 硬编码 `"io.debezium.embedded.ConvertingEngineBuilderFactory"` |
| [3.6.0.Final](https://github.com/debezium/debezium/tree/v3.6.0.Final/debezium-embedded/src/main/java/io/debezium/embedded) | **`EmbeddedEngine`、`ConvertingEngineBuilder` 已被删除**，只剩 `async/` 下的 `AsyncEmbeddedEngine`、`ConvertingAsyncEngineBuilderFactory` 等 | 硬编码 `"io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory"` |

3.6 的默认选择逻辑（[DebeziumEngine.java](https://github.com/debezium/debezium/blob/main/debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java)）：

```java
private static BuilderFactory determineBuilderFactory() {
    return determineBuilderFactory("io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory");
}
```

即通过 `ServiceLoader<BuilderFactory>` 加载全部实现，再按**类名字符串**匹配。这带来一个打包上的硬性要求：**shade/assembly 时必须合并 `META-INF/services`**，否则 `ServiceLoader` 找不到实现，会抛 `DebeziumException("No implementation of Debezium engine builder was found")`。官方文档专门有 "Packaging your project" 一节讲这个，要求加 `ServicesResourceTransformer`（Maven Shade）或 `metaInf-services` 描述符（Maven Assembly）。**这是我们打 fat-jar 时最容易踩的坑。**

### 1.3 `create(...)` 签名演进

1.9.8.Final（[源码](https://github.com/debezium/debezium/blob/v1.9.8.Final/debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java)）只有 4 个 `create`：

```java
static <T> Builder<ChangeEvent<T,T>> create(Class<? extends SerializationFormat<T>> format)
static <K,V> Builder<ChangeEvent<K,V>> create(Class<? extends SerializationFormat<K>> keyFormat,
                                              Class<? extends SerializationFormat<V>> valueFormat)
static ... create(KeyValueChangeEventFormat<K,V> format)
static ... create(ChangeEventFormat<V> format)
```

3.x 在此之上新增：

```java
// header 格式（第三个 format 参数）
static <K,V,H> Builder<ChangeEvent<K,V>> create(Class<? extends SerializationFormat<K>> keyFormat,
                                                Class<? extends SerializationFormat<V>> valueFormat,
                                                Class<? extends SerializationFormat<H>> headerFormat)
// 显式指定引擎实现（builderFactory 类名）
static <K,V,H> Builder<ChangeEvent<K,V>> create(..., String builderFactory)
static ... create(KeyValueHeaderChangeEventFormat<K,V,H> format)
static ... create(KeyValueHeaderChangeEventFormat<K,V,H> format, String builderFactory)
```

`String builderFactory` 重载是 2.6 引入的、用来在 2.6–3.1 期间显式切到异步引擎的开关；3.2 之后默认即异步，这个重载变成"锁定实现"的用途。

`Builder` 接口本身从 1.9 到 3.6 基本没变（`notifying(Consumer)` / `notifying(ChangeConsumer)` / `using(Properties|ClassLoader|Clock|CompletionCallback|ConnectorCallback|OffsetCommitPolicy)` / `build()`），3.x 增量是 `default Builder<R> shutdown(Shutdown<R>)`（`@Incubating`）。

### 1.4 `Consumer` vs `ChangeConsumer`

- `Builder.notifying(java.util.function.Consumer<R>)`：逐条回调，最简单。文档警告：handler 不应抛异常，抛了引擎只会记日志并继续处理下一条，应用可能与数据库产生不一致。
- `Builder.notifying(DebeziumEngine.ChangeConsumer<R>)`：批量回调 `void handleBatch(List<R> records, RecordCommitter<R> committer)`，另有 `default boolean supportsTombstoneEvents()`。**这是我们应当采用的形式**——批量写下游 + 显式提交，才能把"投递成功"与"offset 提交"绑定起来（可靠性要求）。

`RecordCommitter<R>` 语义（[接口源码](https://github.com/debezium/debezium/blob/main/debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java)，官方文档说明其 **thread-safe**）：

```java
void markProcessed(R record) throws InterruptedException;           // 每条都必须调
void markBatchFinished() throws InterruptedException;               // 每批结束调，可能触发 offset flush
void markProcessed(R record, Offsets sourceOffsets);                // 可覆写该记录的 source offset
Offsets buildOffsets();                                             // 构造可写的 Offsets
```

注意：`markBatchFinished()` 只是"允许 flush"，实际是否落盘由 `offset.commit.policy`（默认 `PeriodicCommitOffsetPolicy`）与 `offset.flush.interval.ms`（默认 60000）决定。官方 "Handling Failures" 一节明确：嵌入式引擎给的是 **at-least-once**，崩溃重启后可能重复 `n * m` 条（n=批大小，m=一个 flush 周期内的批数）；要 exactly-once 得用完整的 Debezium 平台。**这条要写进我们的 CONTEXT.md 语义约定。**

### 1.5 异步引擎的可调参数（3.2+ 全部生效）

来自 [engine.html "Asynchronous Engine Properties"](https://debezium.io/documentation/reference/stable/development/engine.html)：

| 属性 | 默认 | 说明 |
| --- | --- | --- |
| `record.processing.threads` | 按负载动态（上限=CPU 核数） | 指定则用固定线程池；可填 `AVAILABLE_CORES` |
| `record.processing.shutdown.timeout.ms` | 1000 | task 停止后等待处理已提交记录的最长时间 |
| `record.processing.order` | `ORDERED` | `ORDERED` 保序；`UNORDERED` 吞吐更好但乱序。**提供 `ChangeConsumer` 时该项无效** |
| `record.processing.with.serial.consumer` | false | 是否把 `Consumer` 包成串行 `ChangeConsumer`。提供 `ChangeConsumer` 时无效 |
| `task.management.timeout.ms` | 180000 | task 启停生命周期操作超时 |

对我们的意义：达梦 CDC 大概率是单 task（`AsyncEmbeddedEngine` 的多 task 只对 SQL Server / MongoDB 这类天然分区的连接器有意义），所以异步引擎带来的收益主要是 **SMT + 序列化的并行化**，而非多 task 并行。若我们用 `ChangeConsumer`，`record.processing.order` 与 `record.processing.with.serial.consumer` 都不起作用——保序由我们自己在 `handleBatch` 里保证。

### 1.6 `ConvertingEngineBuilder`

1.9–3.1 的 `io.debezium.embedded.ConvertingEngineBuilder` 是"在 `EmbeddedEngine` 外面套一层做 Kafka Connect Converter 序列化"的装饰器（把 `SourceRecord` 转成 `ChangeEvent<String,String>` 等）。3.2 起被 `io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory` 取代，序列化被下沉到并行的 `ParallelSmtAndConvert*Processor` 里（见 [DDD-7](https://github.com/debezium/debezium-design-documents/blob/main/DDD-7.md) 的 "Processing CDC events concurrently"）。**结论：`ConvertingEngineBuilder` 不是我们该引用的类，它是内部实现，且 3.2+ 已不存在。**

---

## 2. 版本选择的成本

### 2.1 JDK 基线（已逐条核对 POM，不是猜的）

| 系列 | `maven.compiler.*` / 目标字节码 | 构建所需 JDK | 来源 |
| --- | --- | --- | --- |
| 1.9 | `source/target = 1.8` | `jdk.min.version = 11` | [v1.9.8.Final/pom.xml](https://github.com/debezium/debezium/blob/v1.9.8.Final/pom.xml) |
| 2.0 | `maven.compiler.release = 11` | `jdk.min.version = 11` | [v2.0.0.Final/pom.xml](https://github.com/debezium/debezium/blob/v2.0.0.Final/pom.xml) |
| 2.7 | `release = 11`（`source=17, target=11`） | `jdk.min.version = 11` | [v2.7.4.Final/pom.xml](https://github.com/debezium/debezium/blob/v2.7.4.Final/pom.xml) |
| 3.0 – 3.6 | `debezium.java.connector.target = 17`（Server/Operator/Outbox = 21） | `debezium.java.source = jdk.min.version = 21` | [v3.6.0.Final/pom.xml](https://github.com/debezium/debezium/blob/v3.6.0.Final/pom.xml)、[v3.0.0.Final/pom.xml](https://github.com/debezium/debezium/blob/v3.0.0.Final/pom.xml) |

官方 [Releases Overview](https://debezium.io/releases/) 的 Tested Versions 表与之一致：3.6 / 3.5 = "17+ for connectors; 21+ for Debezium Server, Operator, Outbox and Quarkus extension"；2.7 = "11+"。

[Debezium 3.0.0.Final 公告](https://debezium.io/blog/2024/10/02/debezium-3-0-final-released/)原文：

> All Debezium connectors require a runtime baseline of Java 17. If you are using Debezium Server, Operator, or the Quarkus Outbox Extension, a runtime baseline of Java 21 is required. If you intend to build Debezium from source, all Debezium projects require Java 21 and Maven 3.9.8 or later.

**注意**：1.9 虽然产出 Java 8 字节码，但**构建**它需要 JDK 11。也就是说 1.9 = 运行时 JDK 8 可用、构建需 JDK 11。

### 2.2 Kafka Connect 依赖版本

| Debezium | `version.kafka` | 来源 |
| --- | --- | --- |
| 1.9.8 | 3.2.0 | v1.9.8.Final/pom.xml |
| 2.0.0 | 3.3.1 | v2.0.0.Final/pom.xml |
| 2.7.4 | 3.7.0 | v2.7.4.Final/pom.xml |
| 3.0.0 | 3.8.0 | v3.0.0.Final/pom.xml |
| 3.6.0 | **4.3.0** | v3.6.0.Final/pom.xml |

[Releases Overview](https://debezium.io/releases/) 的兼容性行：3.6 / 3.5 = "Kafka Connect 3.1 and later"；2.7 = "2.x, 3.x"。3.2 系列的一个 highlight 就是 "Support for Kafka 4.x"。

### 2.3 维护 / EOL 状态

Debezium **没有公开的正式 LTS/EOL 政策页面**（详见「未证实事项」）。可依据的一手信号：

- [Releases Overview](https://debezium.io/releases/) 侧边导航只列出 **3.6（latest stable）、3.5（stable）、2.7（stable）**；Tested Versions 矩阵也只有这三列。
- 各系列最后一次发布（Maven Central 目录时间戳 + 官方 release notes）：
  - 1.9.8.Final —— **2023-12-15**（[1.9 release notes](https://debezium.io/releases/1.9/release-notes)），Maven Central 目录同日期。
  - 2.7.4.Final —— **2024-12-11**（[repo1 目录列表](https://repo1.maven.org/maven2/io/debezium/debezium-core/)）。
  - 3.6.0.Final —— **2026-07-01**。
- 主干 `main` 已是 `3.7.0-SNAPSHOT`（[main/pom.xml](https://github.com/debezium/debezium/blob/main/pom.xml)）。

**判断：1.9 已停更约 2.5 年，2.7 已停更约 1.5 年、且是 2.x 线唯一还挂着 "stable" 标签的。新项目选 1.9 等于一上来就欠 2 个大版本的技术债，且拿不到任何安全修复。**

### 2.4 从 1.9 迁到 3.x 的 breaking changes（对我们几乎全是"不适用"）

因为我们是**全新连接器、无存量 offset/schema history**，下列断裂点对我们只是"直接按新写法写"，不构成迁移成本：

| 断裂点 | 1.9 | 2.0+ | 证据 |
| --- | --- | --- | --- |
| Schema 历史接口 | `io.debezium.relational.history.DatabaseHistory` | `…history.SchemaHistory` | [1.9 DatabaseHistory.java](https://github.com/debezium/debezium/blob/v1.9.8.Final/debezium-core/src/main/java/io/debezium/relational/history/DatabaseHistory.java) vs [2.0 SchemaHistory.java](https://github.com/debezium/debezium/blob/v2.0.0.Final/debezium-core/src/main/java/io/debezium/relational/history/SchemaHistory.java) |
| Schema 历史配置前缀 | `database.history.` | `schema.history.internal.` | 上述两文件的 `CONFIGURATION_FIELD_PREFIX_STRING` 常量 |
| 逻辑名配置 | `database.server.name` | `topic.prefix` | [2.0 CommonConnectorConfig.java 第 317 行 `Field.create("topic.prefix")`](https://github.com/debezium/debezium/blob/v2.0.0.Final/debezium-core/src/main/java/io/debezium/config/CommonConnectorConfig.java) |
| 嵌入式引擎实现 | `EmbeddedEngine` | 3.2+ 仅 `AsyncEmbeddedEngine` | 见 1.2 节 |
| 模块拆分 | `debezium-core` 是单体 | **3.5 起** 拆出 `debezium-config` / `debezium-connector-common` / `debezium-connect-plugins`，`debezium-core` 退化为只含 `pom.xml` 的门面 | [v3.0.0.Final/debezium-core 有 src](https://github.com/debezium/debezium/tree/v3.0.0.Final/debezium-core) vs [v3.6.0.Final/debezium-core 只有 pom.xml](https://github.com/debezium/debezium/tree/v3.6.0.Final/debezium-core) |

**模块拆分这一条对我们是利好**：3.5+ 可以只依赖 `debezium-connector-common`，避开 `debezium-connect-plugins`（194 KB 的 SMT / CloudEvents 插件）。这是选 3.5/3.6 而不是 3.0–3.4 的一个具体理由。

---

## 3. 依赖足迹

### 3.1 三个核心 artifact 各自拖什么（3.6.0.Final 实测 POM）

**`io.debezium:debezium-api`** —— [POM](https://repo1.maven.org/maven2/io/debezium/debezium-api/3.6.0.Final/debezium-api-3.6.0.Final.pom)

```
compile: org.slf4j:slf4j-api, io.debezium:debezium-util
test:    logback-classic, junit-jupiter, assertj-core, org.apache.kafka:connect-api
```

**注意：`connect-api` 在 `debezium-api` 里是 `test` scope。** 也就是说 `debezium-api` 本身是干净的、不依赖 Kafka。这也是它能作为对外 API 门面的原因。

**`io.debezium:debezium-core`**（3.6）—— [POM](https://repo1.maven.org/maven2/io/debezium/debezium-core/3.6.0.Final/debezium-core-3.6.0.Final.pom)

```
compile: debezium-connector-common, debezium-config, debezium-connect-plugins
```

无源码，纯聚合门面。**我们不应该依赖它**（会白拖 `debezium-connect-plugins`）。

**`io.debezium:debezium-connector-common`**（真正的连接器框架）—— [POM](https://repo1.maven.org/maven2/io/debezium/debezium-connector-common/3.6.0.Final/debezium-connector-common-3.6.0.Final.pom)

```
compile:  debezium-api, debezium-util, debezium-config,
          jackson-core, jackson-databind, jackson-datatype-jsr310,
          io.debezium:debezium-openlineage-api, com.datadoghq:sketches-java
provided: slf4j-api, connect-api, connect-transforms, connect-json, opentelemetry-api
```

`provided` 意味着**使用方必须自己提供 `connect-api`**，它不会自动传递过来。

**`io.debezium:debezium-embedded`** —— [POM](https://repo1.maven.org/maven2/io/debezium/debezium-embedded/3.6.0.Final/debezium-embedded-3.6.0.Final.pom)

```
compile: debezium-connector-common, slf4j-api,
         org.apache.kafka:connect-api,
         org.apache.kafka:connect-runtime  (排除 kafka-log4j-appender, log4j)
         org.apache.kafka:connect-json,
         org.apache.kafka:connect-file
```

1.9.8 的 [debezium-embedded POM](https://repo1.maven.org/maven2/io/debezium/debezium-embedded/1.9.8.Final/debezium-embedded-1.9.8.Final.pom) 形状**完全一致**（`debezium-core + slf4j + connect-api + connect-runtime + connect-json + connect-file`）。**所以"用 1.9 依赖树更轻"是错觉——1.9 到 3.6 的嵌入式依赖形状没变，只是 Kafka 版本从 3.2.0 涨到 4.3.0。**

### 3.2 `connect-api` 能不能去掉？—— **不能，且这是 Debezium 的显式设计决定**

三重证据：

1. **官方设计文档把它列为 Non-goal。** [DDD-7](https://github.com/debezium/debezium-design-documents/blob/main/DDD-7.md) "Non-goals" 一节直接写着 `Remove dependency on Kafka Connect API.`，并在 "Preserving Kafka Connect model" 小节解释了为什么：

   > removing `WorkerConfig` would require removing `OffsetBackingStore`, which would require removing `OffsetStorageReader` etc., etc., resulting in substantial changes in the Debezium core and connectors. Therefore, this should be done in a separate task which would deserve a dedicated DDD…

2. **类型层面渗透。** Debezium 的事件信封 [`io.debezium.data.Envelope`](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-connector-common/src/main/java/io/debezium/data/Envelope.java) 直接 import `org.apache.kafka.connect.data.{Field,Schema,Struct}` 和 `org.apache.kafka.connect.source.SourceRecord`；[`BaseSourceTask`](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-connector-common/src/main/java/io/debezium/connector/common/BaseSourceTask.java) `extends org.apache.kafka.connect.source.SourceTask` 且 import `org.apache.kafka.clients.producer.RecordMetadata`。

3. **`connect-api` 自身 compile 依赖 `kafka-clients`**（[connect-api-4.3.0.pom](https://repo1.maven.org/maven2/org/apache/kafka/connect-api/4.3.0/connect-api-4.3.0.pom)），所以留 `connect-api` 就必然带上 ~10 MB 的 `kafka-clients`。

### 3.3 `connect-runtime` 能不能去掉？—— **用 `debezium-embedded` 就不能**

[`AsyncEmbeddedEngine.java`](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-embedded/src/main/java/io/debezium/embedded/async/AsyncEmbeddedEngine.java) 的 Kafka import 清单（实测）：

```java
org.apache.kafka.connect.runtime.AbstractHerder
org.apache.kafka.connect.runtime.ConnectorConfig
org.apache.kafka.connect.runtime.WorkerConfig
org.apache.kafka.connect.runtime.rest.entities.ConfigInfo
org.apache.kafka.connect.runtime.rest.entities.ConfigInfos
org.apache.kafka.connect.storage.FileOffsetBackingStore
org.apache.kafka.connect.storage.KafkaOffsetBackingStore
org.apache.kafka.connect.storage.MemoryOffsetBackingStore
org.apache.kafka.connect.storage.OffsetBackingStore
org.apache.kafka.connect.storage.OffsetStorageReaderImpl
org.apache.kafka.connect.storage.OffsetStorageWriter
org.apache.kafka.connect.util.ConnectorTaskId
org.apache.kafka.connect.util.LoggingContext
```

已核对：`Converter` / `HeaderConverter` / `OffsetStorageReader` 属于 **connect-api**（[connect/api/.../storage](https://github.com/apache/kafka/tree/4.1/connect/api/src/main/java/org/apache/kafka/connect/storage)）；`OffsetBackingStore` / `FileOffsetBackingStore` / `KafkaOffsetBackingStore` / `MemoryOffsetBackingStore` / `OffsetStorageReaderImpl` / `OffsetStorageWriter` 属于 **connect-runtime**（[connect/runtime/.../storage](https://github.com/apache/kafka/tree/4.1/connect/runtime/src/main/java/org/apache/kafka/connect/storage)）。

### 3.4 可以裁掉的部分

[connect-runtime-4.3.0.pom](https://repo1.maven.org/maven2/org/apache/kafka/connect-runtime/4.3.0/connect-runtime-4.3.0.pom) 的依赖 scope：

- `compile`：`connect-api`、`kafka-clients`、`connect-json`、`connect-transforms` —— 保留。
- `runtime`：`jetty-server`、`jetty-ee10-servlet`、`jetty-ee10-servlets`、`jetty-client`、`jersey-container-servlet`、`jersey-hk2`、`jackson-jakarta-rs-json-provider`、`jose4j`、`jaxb-api`、`activation`、`classgraph`、`maven-artifact`、`swagger-annotations` —— **这些是 Connect REST server / 插件扫描用的，嵌入式模式不启动 REST server，理论上全部可 exclude。**

另外可裁：

- `org.apache.kafka:connect-file`（18 KB，`FileStreamSourceConnector`，我们用不到）。
- `kafka-clients` 的 runtime 传递依赖 `zstd-jni` / `lz4-java` / `snappy-java`（[kafka-clients-4.3.0.pom](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.3.0/kafka-clients-4.3.0.pom)），**前提是不使用 `KafkaOffsetBackingStore` / `KafkaSchemaHistory`**。
- `io.debezium:debezium-ddl-parser`（**6.2 MB**，ANTLR 生成的 MySQL/Oracle DDL 语法）—— 只有当我们要跟随 Oracle 连接器那套 DDL 解析时才需要。达梦若走 LogMiner-like 路线可能需要，走"表结构快照 + JDBC metadata"路线则完全不需要。**这是我们能省下的最大一块，务必在架构上避开。**
- `io.debezium:debezium-connect-plugins`（194 KB，SMT/CloudEvents）—— 只要不依赖 `debezium-core` 门面即可避开。

### 3.5 实测 jar 体积（Maven Central `Content-Length`，3.6.0.Final / Kafka 4.3.0）

| Artifact | 大小 | 最小集是否需要 |
| --- | --- | --- |
| `io.debezium:debezium-api` | 31 KB | ✅ |
| `io.debezium:debezium-config` | 76 KB | ✅ |
| `io.debezium:debezium-util` | 171 KB | ✅ |
| `io.debezium:debezium-connector-common` | 992 KB | ✅ |
| `io.debezium:debezium-openlineage-api` | 17 KB | ✅（connector-common compile 依赖） |
| `com.datadoghq:sketches-java` | 126 KB | ✅（同上） |
| `io.debezium:debezium-embedded` | 109 KB | 方案 A/B ✅，方案 C ❌ |
| `io.debezium:debezium-storage-file` | 8 KB | 可选（FileSchemaHistory） |
| `io.debezium:debezium-connect-plugins` | 194 KB | ❌ 可避开 |
| `io.debezium:debezium-ddl-parser` | **6222 KB** | ❌ 应避开 |
| `org.apache.kafka:connect-api` | 111 KB | ✅ 不可去 |
| `org.apache.kafka:kafka-clients` | **9961 KB** | ✅ 不可去（connect-api compile 依赖） |
| `org.apache.kafka:connect-runtime` | 817 KB | 方案 A/B ✅，方案 C ❌ |
| `org.apache.kafka:connect-json` | 35 KB | ✅（JSON 输出格式） |
| `org.apache.kafka:connect-transforms` | 118 KB | 用 SMT 才需要 |
| `org.apache.kafka:connect-file` | 18 KB | ❌ 可 exclude |

加上 Jackson（core / databind / annotations / datatype-jsr310）与 slf4j-api，**方案 A/B 的最小集约 15 MB，其中 `kafka-clients` 独占约 2/3。**

### 3.6 三个方案对比

| | 方案 A：直接用 `debezium-embedded` | 方案 B：用 `debezium-embedded` + 激进 exclude | 方案 C：自研 mini engine 驱动 `SourceTask` |
| --- | --- | --- | --- |
| 依赖 | connector-common + embedded + connect-runtime + kafka-clients + 全部 runtime 传递依赖 | 同 A，但 exclude Jetty/Jersey/jose4j/classgraph/maven-artifact/swagger/connect-file/压缩编解码 | connector-common + connect-api + kafka-clients + 自研 offset store |
| 体积量级 | ~20 MB+ | **~15 MB** | ~12 MB |
| `DebeziumEngine` API | 原生 | 原生 | 需自己实现 `DebeziumEngine` 或另立门面 |
| SMT / 多种输出格式 | 免费 | 免费 | 需自己实现 |
| 维护成本 | 低 | 低（需回归测试确认 exclude 无误） | **高**，且要自己复刻 `OffsetStorageWriter` 的 flush 语义 |

**推荐方案 B。** 方案 C 省下的 3 MB 换来的是自己维护 offset 提交语义 —— 对一个"以可靠性为卖点"的库来说不划算。若后续确有极端体积要求，再把方案 C 作为 v2 目标。

---

## 4. 可复用组件逐项评估

### 4.1 `OffsetBackingStore`

- 接口：`org.apache.kafka.connect.storage.OffsetBackingStore`，位于 **`connect-runtime`**（[Kafka 源码](https://github.com/apache/kafka/tree/4.1/connect/runtime/src/main/java/org/apache/kafka/connect/storage)）。
- 三个内置实现全部在 `connect-runtime`：`MemoryOffsetBackingStore`、`FileOffsetBackingStore`、`KafkaOffsetBackingStore`。
- 配置方式（[engine.html Engine Properties](https://debezium.io/documentation/reference/stable/development/engine.html)）：`offset.storage`（默认 `…FileOffsetBackingStore`）、`offset.storage.file.filename`、`offset.storage.topic` / `.partitions` / `.replication.factor`（Kafka 版专用）、`offset.commit.policy`（默认 `PeriodicCommitOffsetPolicy`）、`offset.flush.interval.ms`（60000）、`offset.flush.timeout.ms`（5000）。
- **与 Kafka Connect 的耦合度：高。** 自定义实现也逃不掉——Debezium 自己的 [`JdbcOffsetBackingStore`](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-storage/debezium-storage-jdbc/src/main/java/io/debezium/storage/jdbc/offset/JdbcOffsetBackingStore.java) 就 `implements org.apache.kafka.connect.storage.OffsetBackingStore` 并 import `org.apache.kafka.connect.runtime.WorkerConfig`。
- **能否独立使用：不能脱离 `connect-runtime`。**
- **对我们的启示**：如果需要"offset 存进达梦自己的表"，照抄 `JdbcOffsetBackingStore` 的模式最省力，Debezium 已有 `debezium-storage-jdbc` / `-redis` / `-rocksdb` / `-s3` / `-azure-blob` / `-rocketmq` / `-chronicle-queue` / `-configmap` 等现成实现（[debezium-storage 模块列表](https://github.com/debezium/debezium/tree/v3.6.0.Final/debezium-storage)）。

### 4.2 Schema History

- **重命名发生在 2.0**：`DatabaseHistory` → `SchemaHistory`，配置前缀 `database.history.` → `schema.history.internal.`（证据见 2.4 节的两份源码常量对比）。
- 3.5+ 接口位置：`io.debezium.relational.history.SchemaHistory`，模块 **`debezium-connector-common`**（[源码](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-connector-common/src/main/java/io/debezium/relational/history/SchemaHistory.java)）。
- **耦合度：低。** `SchemaHistory.java` 对 Kafka 的依赖只有 `org.apache.kafka.common.config.ConfigDef`（来自 `kafka-clients`），**不碰 `connect-runtime`**。`debezium-storage-file` 的 [POM](https://repo1.maven.org/maven2/io/debezium/debezium-storage-file/3.6.0.Final/debezium-storage-file-3.6.0.Final.pom) 只有 `debezium-api` + `debezium-connector-common` + `slf4j-api`(provided) + `connect-api`(provided)，jar 仅 8 KB。
- **能否独立使用：能。** 文件实现 `io.debezium.storage.file.history.FileSchemaHistory` 完全不需要 Kafka broker。默认值 `…KafkaSchemaHistory` 需要 Kafka 集群，我们必须显式覆盖成 `FileSchemaHistory`（或自研 JDBC 版）。
- 达梦连接器若需要 DDL 历史，直接用 `debezium-storage-file` 即可，代价可忽略。

### 4.3 `SourceRecord` 事件信封与 `Envelope`

- `org.apache.kafka.connect.source.SourceRecord` 来自 `connect-api`。
- Debezium 的信封结构由 `io.debezium.data.Envelope` 定义（3.5+ 在 `debezium-connector-common`，[源码](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-connector-common/src/main/java/io/debezium/data/Envelope.java)），产出 `before` / `after` / `source` / `op` / `ts_ms` 结构的 `Struct`。
- **耦合度：高**（直接 import Kafka Connect 的 `Schema` / `Struct` / `Field` / `SourceRecord`），**但这正是我们要的**——沿用 Debezium 标准信封，下游生态（Flink CDC、Debezium Server、各类 JSON 消费者）可以零成本对接。
- 引擎输出格式（[engine.html Output Message Formats](https://debezium.io/documentation/reference/stable/development/engine.html)）：`Connect.class`（原始 `SourceRecord`）、`Json.class`、`JsonByteArray.class`、`Avro.class`、`CloudEvents.class`；header 可选 `Json.class` / `JsonByteArray.class`。

### 4.4 Converter 体系

- Kafka Connect 侧：`org.apache.kafka.connect.storage.Converter` / `HeaderConverter` SPI 在 **`connect-api`**；`org.apache.kafka.connect.json.JsonConverter` 在 **`connect-json`**（35 KB）。引擎内部把序列化委派给 Kafka Connect 或 Apicurio 的 converter 实现（文档原文："Internally, the engine delegates data conversion to the Kafka Connect or Apicurio converter implementation"），可用 `converter.*` 引擎属性调参，例如 `converter.schemas.enable=false`。
- Debezium 侧的**自定义类型转换** SPI 是 `io.debezium.spi.converter.CustomConverter`，位于 **`debezium-api`** 且标注 `@Incubating`（[源码](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-api/src/main/java/io/debezium/spi/converter/CustomConverter.java)），文档见 [Custom Converters](https://debezium.io/documentation/reference/stable/development/converters.html)。**达梦特有类型（如 TIMESTAMP WITH TIME ZONE、CLOB/BLOB、自定义精度 NUMBER）的映射就走这条路。**
- **没有名为 `debezium-converters` 的模块**（[3.6.0.Final 根目录模块清单](https://github.com/debezium/debezium/tree/v3.6.0.Final)）。SMT 与 CloudEvents converter 在 `debezium-connect-plugins`。
- Avro / Apicurio：`io.confluent:kafka-connect-avro-converter` 与 `io.apicurio:apicurio-registry-utils-converter` 在 `debezium-embedded` / `debezium-connector-common` 中均为 **`test` scope**，不进运行时依赖树。**我们默认不支持 Avro，把它做成可选扩展即可**（Confluent 的 Community License 不是 Apache 2.0，见第 6 节）。
- **能否独立使用：`JsonConverter` 只需 `connect-json`（35 KB）+ `connect-api`，很轻，建议作为默认输出格式。**

---

## 5. 写自己的连接器需要实现哪些 SPI

### 5.1 最权威的参照物：Debezium 官方的**外置连接器仓库**

[Debezium GitHub 组织](https://github.com/orgs/debezium/repositories) 里有一批独立仓库的连接器，都是"在 Debezium 之外写一个新数据库连接器"的现成模板：

`debezium-connector-yashandb`（崖山，国产）、`debezium-connector-tidb`、`debezium-connector-cockroachdb`、`debezium-connector-informix`、`debezium-connector-ingres`、`debezium-connector-sqlite`、`debezium-connector-neo4j`、`debezium-connector-milvus`、`debezium-connector-spanner`、`debezium-connector-cassandra`、`debezium-connector-ibmi`、`debezium-connector-db2`、`debezium-connector-jdbc`。

**[`debezium-connector-yashandb`](https://github.com/debezium/debezium-connector-yashandb) 是最贴近我们的参照**：同为国产关系库、走 JDBC + 专有流式接口、已被官方文档收录（[Releases Overview](https://debezium.io/releases/) 的 Tested Versions 表里有 YashanDB 一行：Database 23.4 / Driver 1.9.27）。

它的 [pom.xml](https://github.com/debezium/debezium-connector-yashandb/blob/main/pom.xml) 依赖形态就是我们应该照抄的：

```
parent:   io.debezium:debezium-parent 3.7.0-SNAPSHOT
compile:  debezium-connector-common, debezium-config, debezium-util,
          debezium-ddl-parser, debezium-connect-plugins, debezium-storage-kafka,
          antlr4-runtime, <厂商 JDBC 驱动>, <厂商流式 SDK>
provided: org.apache.kafka:connect-api, org.slf4j:slf4j-api
test:     debezium-embedded, debezium-connector-common(test-jar), debezium-util(test-jar)
```

**注意 `debezium-embedded` 只在 test scope。** 这印证了第 3 节的结论：连接器本体不该 compile-依赖引擎。

### 5.2 需要实现的类清单（以 YashanDB 连接器 [src 目录](https://github.com/debezium/debezium-connector-yashandb/tree/main/src/main/java/io/debezium/connector/yashandb) 实测为准）

| 我们要写的类 | 继承/实现 | 所在模块 |
| --- | --- | --- |
| `DmConnector` | `io.debezium.connector.common.RelationalBaseSourceConnector`（其上游是 Kafka `SourceConnector`），并 `implements io.debezium.metadata.ConfigDescriptor` | connector-common |
| `DmConnectorTask` | `io.debezium.connector.common.BaseSourceTask<P, O>`（`extends org.apache.kafka.connect.source.SourceTask`） | connector-common |
| `DmConnectorConfig` | `io.debezium.relational.RelationalDatabaseConnectorConfig` | connector-common |
| `DmConnection` | `io.debezium.jdbc.JdbcConnection` 系 | connector-common |
| `DmDatabaseSchema` | `io.debezium.relational.HistorizedRelationalDatabaseSchema` / `RelationalDatabaseSchema` | connector-common |
| `DmPartition` / `DmOffsetContext` | `io.debezium.pipeline.spi.Partition` / `OffsetContext` | connector-common |
| `SourceInfo` / `DmSourceInfoStructMaker` | `io.debezium.connector.*SourceInfo` / `SourceInfoStructMaker` | connector-common |
| `DmSnapshotChangeEventSource` | `RelationalSnapshotChangeEventSource` | connector-common |
| 流式实现（`StreamingAdapter` 风格） | `io.debezium.pipeline.source.spi.StreamingChangeEventSource` | connector-common |
| `DmChangeEventSourceFactory` | `io.debezium.pipeline.ChangeEventSourceFactory` | connector-common |
| `DmValueConverters` | `io.debezium.relational.ValueConverterProvider` | connector-common |
| `DmDefaultValueConverter` | `io.debezium.relational.DefaultValueConverter` | connector-common |
| `DmErrorHandler` | `io.debezium.pipeline.ErrorHandler` | connector-common |
| `DmEventMetadataProvider` | `io.debezium.pipeline.source.spi.EventMetadataProvider` | connector-common |
| `DmSchemaFactory` | `io.debezium.schema.SchemaFactory` | connector-common |
| `DmTaskContext` | `io.debezium.connector.common.CdcSourceTaskContext` | connector-common |
| `Module` | 版本/名称元数据（约定俗成的小类） | — |
| `metadata/` 包 | `io.debezium.metadata.ConnectorDescriptor` 等 | connector-common |
| `DmChangeEventSourceMetricsFactory` + MXBean | `ChangeEventSourceMetricsFactory` | connector-common |

**框架自己提供、我们只需组装的**（从 [`YashanDbConnectorTask` 的 import 清单](https://github.com/debezium/debezium-connector-yashandb/blob/main/src/main/java/io/debezium/connector/yashandb/YashanDbConnectorTask.java) 可以完整读出）：

`ChangeEventSourceCoordinator`、`EventDispatcher`、`ChangeEventQueue`、`QueueProviderService`、`ErrorHandler`、`SignalProcessor`、`NotificationService`、`HeartbeatFactory`、`SnapshotterService` / `Snapshotter` SPI、`TopicNamingStrategy`、`SchemaNameAdjuster`、`CustomConverterRegistry`、`DebeziumHeaderProducer`、`MainConnectionProvidingConnectionFactory`、`StandardBeanNames` bean 注册表。

**这就是复用 Debezium 的真正价值所在**：增量快照、信号通道、通知、心跳、指标、topic 命名、错误重试这些全部白拿。

### 5.3 哪些是公开 API、哪些是内部不稳定实现（重要）

**Debezium 只对 `debezium-api` 做 API 兼容性校验。** 证据：`debezium-parent/pom.xml` 设 `<revapi.skip>true</revapi.skip>`（[源码第 59 行](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-parent/pom.xml)），而 `debezium-api/pom.xml` 单独把它翻回来：

```xml
<properties>
    <!-- Enable API checks in this module -->
    <revapi.skip>false</revapi.skip>
</properties>
```
（[debezium-api-3.6.0.Final.pom](https://repo1.maven.org/maven2/io/debezium/debezium-api/3.6.0.Final/debezium-api-3.6.0.Final.pom)）

**推论：**

- `io.debezium.engine.*`（`debezium-api`）= **稳定公开 API**，可放心对外暴露。
- `io.debezium.connector.common.*`、`io.debezium.pipeline.*`、`io.debezium.relational.*`（`debezium-connector-common`）= **内部实现，小版本间可能 breaking**。我们的连接器深度依赖这些，因此**必须把 Debezium 版本 pin 死并纳入 CI 回归**，不能用范围版本。
- `@io.debezium.common.annotation.Incubating` 标注的成员（如 `DebeziumEngine.Signaler`、`Shutdown`、`CustomConverter`）随时可能改。

### 5.4 官方有没有"连接器开发指南"？—— **没有**

- 官方文档的 "API and SPI" 一节只有两页：**Debezium Engine** 和 **Custom Converters**（见 [engine.html](https://debezium.io/documentation/reference/stable/development/engine.html) 左侧导航）。没有 "connector development" 页面。
- `debezium-design-documents` 仓库的 [README](https://github.com/debezium/debezium-design-documents/blob/main/README.md) 列出全部 DDD：DDD-1（多 source partition）、**DDD-3（增量快照）**、**DDD-7（异步嵌入式引擎）**、DDD-8（只读增量快照）、DDD-9（Oracle unbuffered adapter）、DDD-12、DDD-13、DDD-16、DDD-38。都是特性设计文档，**不是连接器开发教程**。
- 最接近教程的官方博客是 [Deep Dive Into a Debezium Community Connector: The Scylla CDC Source Connector](https://debezium.io/blog/2021/09/22/deep-dive-into-a-debezium-community-connector-scylla-cdc-source-connector/)（2021，基于 1.x，仅供思路参考）。
- **没有找到官方的 `debezium-connector-*` Maven archetype**（见「未证实事项」）。

**实操结论：我们的"开发指南"就是 `debezium-connector-yashandb` 的源码 + `debezium-connector-oracle`（同为需要日志挖掘的关系库）的源码。** 建议在实现前先把这两个仓库的 `*ConnectorTask` / `*ChangeEventSourceFactory` / `*StreamingChangeEventSource` 通读一遍。

---

## 6. 许可证

### 6.1 Debezium 本体：Apache License 2.0（确认）

- [v3.6.0.Final/LICENSE.txt](https://github.com/debezium/debezium/blob/v3.6.0.Final/LICENSE.txt) = 完整 Apache License Version 2.0 文本。
- 每个源文件头都有 `Licensed under the Apache Software License version 2.0`。
- [debezium.io/releases](https://debezium.io/releases/) 页脚："Debezium is open, available under the Apache Software License 2.0."（网站与文档本身另按 CC BY 3.0）。

### 6.2 对我们分发模式的影响

Apache-2.0 是**宽松（permissive）许可证，非 copyleft**。因此：

- ✅ **可以**把 Debezium 作为依赖嵌进我们的库并**闭源分发 / 商业销售**，我们自己写的达梦连接器代码可以采用任意许可证（含商业专有）。
- ⚠️ **必须**履行的义务（Apache-2.0 §4）：
  1. 分发物中附带 Apache-2.0 许可证全文（`LICENSE`）。
  2. 保留原文件中的版权、专利、商标、归属声明。
  3. 若上游有 `NOTICE` 文件，需在我们的 `NOTICE` 中转述其内容。
  4. 若我们修改了 Debezium 源码（例如 fork 打补丁），需在修改过的文件中**显著标注已修改**。
- ⚠️ **商标**：Apache-2.0 不授予商标许可。**不能用 "Debezium" 命名我们的产品或暗示官方背书**（Debezium 有 [Trademark Policy](https://debezium.io/)，页脚有链接）。产品名用 `dm-connector` 即可，文档里说 "based on Debezium" 是描述性使用，属于合理范围。
- ✅ 专利授权：Apache-2.0 §3 提供明确的专利授权，对商业分发是加分项。

**建议：如果走"嵌入式库 + 依赖由使用方从 Maven 拉取"的分发模式（不打 fat-jar），归属义务最轻——只需在文档中声明依赖及其许可证。一旦打 shaded fat-jar，就必须完整生成 LICENSE/NOTICE 聚合文件（可用 `license-maven-plugin`）。**

### 6.3 传递依赖的许可证

| 依赖 | 许可证 | 备注 |
| --- | --- | --- |
| `org.apache.kafka:connect-api` / `connect-runtime` / `connect-json` / `kafka-clients` | Apache-2.0 | [kafka-clients POM 的 `<licenses>`](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.3.0/kafka-clients-4.3.0.pom) 明写 "The Apache License, Version 2.0" |
| `com.fasterxml.jackson.*` | Apache-2.0 | — |
| `org.slf4j:slf4j-api` | MIT | 宽松，兼容 |
| `com.datadoghq:sketches-java` | Apache-2.0 | [POM `<licenses>`](https://repo1.maven.org/maven2/com/datadoghq/sketches-java/0.8.3/sketches-java-0.8.3.pom) 已核实 |
| `com.github.luben:zstd-jni` | **BSD 2-Clause** | `kafka-clients` 的 runtime 依赖。宽松、可商用，但需保留版权声明。**不用 Kafka 存储时可 exclude** |
| `org.lz4:lz4-java`、`org.xerial.snappy:snappy-java` | Apache-2.0 | 同上，可 exclude |
| `org.antlr:antlr4-runtime` | **BSD 3-Clause** | `debezium-ddl-parser` 需要。见 [LICENSE-3rd-PARTIES.txt](https://github.com/debezium/debezium/blob/v3.6.0.Final/LICENSE-3rd-PARTIES.txt)。宽松可商用。**若避开 ddl-parser 则不涉及** |
| Google Protocol Buffers（部分生成代码） | **BSD 3-Clause** | 见 `LICENSE-3rd-PARTIES.txt`；主要与 Postgres decoderbufs 相关，我们不涉及 |
| `io.confluent:kafka-connect-avro-converter` | **Confluent Community License（非 OSI 开源）** | ⚠️ 在 Debezium 中是 `test` scope，**绝不能进我们的运行时依赖树**。若将来要支持 Avro，改用 Apicurio（Apache-2.0） |

**结论：只要（a）避开 `debezium-ddl-parser`、（b）绝不引入 Confluent Avro converter，我们的依赖树就是纯 Apache-2.0 + MIT + BSD 的宽松组合，闭源商业分发无障碍。** BSD/MIT 的义务与 Apache-2.0 类似（保留版权声明），照常聚合进 NOTICE 即可。

---

## 未证实事项

1. **Debezium 官方 EOL / 支持周期政策**。未找到 debezium.io 上任何正式的版本生命周期或 LTS 政策页面。多次 `WebSearch`（限定 `debezium.io` 域）只返回各系列的 release notes。本文的"维护状态"判断是从 [Releases Overview](https://debezium.io/releases/) 的导航栏（只列 3.6/3.5/2.7 为 stable）与 Maven Central 的最后发布时间**推断**得出，非官方声明。
2. **`debezium-connector-*` 的 Maven archetype**。未在 Maven Central（`io.debezium` groupId 下）或 debezium GitHub 组织中找到连接器项目脚手架 archetype。若存在，未能通过一手来源确认其坐标。
3. **`debezium.io/releases/` 概览页上 1.9 系列标注的日期 "2022-12-15" 与 [1.9 release notes](https://debezium.io/releases/1.9/release-notes) 中 "Release 1.9.8.Final (December 15th 2023)" 及 Maven Central 目录时间戳 2023-12-15 不一致**。本文采信后两者（2023-12-15）。概览页那个日期的确切含义未证实。
4. **exclude `connect-runtime` 的 Jetty/Jersey 系 runtime 依赖后，`AsyncEmbeddedEngine` 是否 100% 正常工作**。理论分析（不启动 REST server）支持可行，且 `AbstractHerder` / `ConfigInfos` 本身在 `connect-runtime` jar 内，但**未做实际运行验证**。落地时必须跑一次完整集成测试确认无 `NoClassDefFoundError`。
5. **Jackson 各 artifact 的精确版本与体积**。`debezium-parent/pom.xml` 与 `debezium-bom/pom.xml` 中 `${version.jackson}` 的最终取值未定位到（可能来自更上层的 `debezium-build-parent`）。因此第 3.5 节的"~15 MB"总计中 Jackson 部分（约 2 MB 量级）是估算。
6. **达梦（DM）是否有可用的日志挖掘/逻辑复制接口，以及是否需要 `debezium-ddl-parser`**。本文只从依赖角度指出应尽量避开 6.2 MB 的 ddl-parser，具体可行性属于另一个调研主题。
7. **`debezium-embedded` 对 `connect-file` 的 compile 依赖的实际用途**未确认（推测为测试用的 `FileStreamSourceConnector`，但声明为 compile scope）。体积仅 18 KB，影响可忽略。

---

## 参考来源

**官方文档**

- [Debezium Engine（3.6 stable）](https://debezium.io/documentation/reference/stable/development/engine.html)
- [Custom Converters](https://debezium.io/documentation/reference/stable/development/converters.html)
- [Debezium Releases Overview（版本 / JDK / Kafka 兼容矩阵）](https://debezium.io/releases/)
- [Release Notes for Debezium 3.6](https://debezium.io/releases/3.6/release-notes)
- [Release Notes for Debezium 3.0](https://debezium.io/releases/3.0/release-notes)
- [Release Notes for Debezium 2.0](https://debezium.io/releases/2.0/release-notes)
- [Release Notes for Debezium 1.9](https://debezium.io/releases/1.9/release-notes)

**官方博客 / 设计文档**

- [Debezium 3.0.0.Final Released（Java 17/21 基线）](https://debezium.io/blog/2024/10/02/debezium-3-0-final-released/)
- [Debezium asynchronous engine](https://debezium.io/blog/2024/07/08/async-embedded-engine/)
- [DDD-7: Asynchronous Debezium Embedded Engine](https://github.com/debezium/debezium-design-documents/blob/main/DDD-7.md)
- [debezium-design-documents README（DDD 索引）](https://github.com/debezium/debezium-design-documents/blob/main/README.md)
- [Deep Dive Into a Debezium Community Connector: Scylla CDC Source Connector](https://debezium.io/blog/2021/09/22/deep-dive-into-a-debezium-community-connector-scylla-cdc-source-connector/)

**源码（GitHub）**

- [DebeziumEngine.java（main / 3.7-SNAPSHOT）](https://github.com/debezium/debezium/blob/main/debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java)
- [DebeziumEngine.java（v1.9.8.Final）](https://github.com/debezium/debezium/blob/v1.9.8.Final/debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java)
- [DebeziumEngine.java（v2.6.0.Final）](https://github.com/debezium/debezium/blob/v2.6.0.Final/debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java)
- [DebeziumEngine.java（v3.1.0.Final）](https://github.com/debezium/debezium/blob/v3.1.0.Final/debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java)
- [AsyncEmbeddedEngine.java（v3.6.0.Final）](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-embedded/src/main/java/io/debezium/embedded/async/AsyncEmbeddedEngine.java)
- [io.debezium.embedded 包（v3.1.0.Final，含 EmbeddedEngine）](https://github.com/debezium/debezium/tree/v3.1.0.Final/debezium-embedded/src/main/java/io/debezium/embedded)
- [io.debezium.embedded 包（v3.6.0.Final，EmbeddedEngine 已移除）](https://github.com/debezium/debezium/tree/v3.6.0.Final/debezium-embedded/src/main/java/io/debezium/embedded)
- [Envelope.java（v3.6.0.Final）](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-connector-common/src/main/java/io/debezium/data/Envelope.java)
- [BaseSourceTask.java（v3.6.0.Final）](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-connector-common/src/main/java/io/debezium/connector/common/BaseSourceTask.java)
- [SchemaHistory.java（v3.6.0.Final）](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-connector-common/src/main/java/io/debezium/relational/history/SchemaHistory.java)
- [SchemaHistory.java（v2.0.0.Final）](https://github.com/debezium/debezium/blob/v2.0.0.Final/debezium-core/src/main/java/io/debezium/relational/history/SchemaHistory.java)
- [DatabaseHistory.java（v1.9.8.Final）](https://github.com/debezium/debezium/blob/v1.9.8.Final/debezium-core/src/main/java/io/debezium/relational/history/DatabaseHistory.java)
- [CommonConnectorConfig.java（v2.0.0.Final，topic.prefix）](https://github.com/debezium/debezium/blob/v2.0.0.Final/debezium-core/src/main/java/io/debezium/config/CommonConnectorConfig.java)
- [CustomConverter.java（v3.6.0.Final）](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-api/src/main/java/io/debezium/spi/converter/CustomConverter.java)
- [JdbcOffsetBackingStore.java（v3.6.0.Final）](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-storage/debezium-storage-jdbc/src/main/java/io/debezium/storage/jdbc/offset/JdbcOffsetBackingStore.java)
- [debezium-storage 模块列表（v3.6.0.Final）](https://github.com/debezium/debezium/tree/v3.6.0.Final/debezium-storage)
- [Debezium 根 pom.xml（main / v3.6.0.Final / v3.0.0.Final / v2.7.4.Final / v2.0.0.Final / v1.9.8.Final）](https://github.com/debezium/debezium/blob/v3.6.0.Final/pom.xml)
- [debezium-parent/pom.xml（v3.6.0.Final，revapi.skip=true）](https://github.com/debezium/debezium/blob/v3.6.0.Final/debezium-parent/pom.xml)
- [LICENSE.txt（v3.6.0.Final）](https://github.com/debezium/debezium/blob/v3.6.0.Final/LICENSE.txt)
- [LICENSE-3rd-PARTIES.txt（v3.6.0.Final）](https://github.com/debezium/debezium/blob/v3.6.0.Final/LICENSE-3rd-PARTIES.txt)
- [Kafka connect-api storage 包（4.1）](https://github.com/apache/kafka/tree/4.1/connect/api/src/main/java/org/apache/kafka/connect/storage)
- [Kafka connect-runtime storage 包（4.1）](https://github.com/apache/kafka/tree/4.1/connect/runtime/src/main/java/org/apache/kafka/connect/storage)
- [debezium-connector-yashandb](https://github.com/debezium/debezium-connector-yashandb) / [其 pom.xml](https://github.com/debezium/debezium-connector-yashandb/blob/main/pom.xml) / [其源码目录](https://github.com/debezium/debezium-connector-yashandb/tree/main/src/main/java/io/debezium/connector/yashandb)
- [debezium-connector-cockroachdb README](https://github.com/debezium/debezium-connector-cockroachdb/blob/main/README.md)
- [Debezium GitHub 组织仓库列表](https://github.com/orgs/debezium/repositories)

**Maven Central**

- [debezium-embedded maven-metadata.xml（全量版本列表）](https://repo1.maven.org/maven2/io/debezium/debezium-embedded/maven-metadata.xml)
- [debezium-core 目录列表（含发布时间戳）](https://repo1.maven.org/maven2/io/debezium/debezium-core/)
- [debezium-api-3.6.0.Final.pom](https://repo1.maven.org/maven2/io/debezium/debezium-api/3.6.0.Final/debezium-api-3.6.0.Final.pom)
- [debezium-core-3.6.0.Final.pom](https://repo1.maven.org/maven2/io/debezium/debezium-core/3.6.0.Final/debezium-core-3.6.0.Final.pom)
- [debezium-connector-common-3.6.0.Final.pom](https://repo1.maven.org/maven2/io/debezium/debezium-connector-common/3.6.0.Final/debezium-connector-common-3.6.0.Final.pom)
- [debezium-embedded-3.6.0.Final.pom](https://repo1.maven.org/maven2/io/debezium/debezium-embedded/3.6.0.Final/debezium-embedded-3.6.0.Final.pom)
- [debezium-embedded-1.9.8.Final.pom](https://repo1.maven.org/maven2/io/debezium/debezium-embedded/1.9.8.Final/debezium-embedded-1.9.8.Final.pom)
- [debezium-storage-file-3.6.0.Final.pom](https://repo1.maven.org/maven2/io/debezium/debezium-storage-file/3.6.0.Final/debezium-storage-file-3.6.0.Final.pom)
- [connect-api-4.3.0.pom](https://repo1.maven.org/maven2/org/apache/kafka/connect-api/4.3.0/connect-api-4.3.0.pom)
- [connect-runtime-4.3.0.pom](https://repo1.maven.org/maven2/org/apache/kafka/connect-runtime/4.3.0/connect-runtime-4.3.0.pom)
- [kafka-clients-4.3.0.pom](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.3.0/kafka-clients-4.3.0.pom)
- [sketches-java-0.8.3.pom](https://repo1.maven.org/maven2/com/datadoghq/sketches-java/0.8.3/sketches-java-0.8.3.pom)
