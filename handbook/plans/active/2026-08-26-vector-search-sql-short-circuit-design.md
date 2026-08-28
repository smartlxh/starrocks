# StarRocks SQL 向量检索短路执行设计

- Status: active
- Owner: Vector Search
- Last Updated: 2026-08-26
- Target: 第一版工程实现（MVP）
- External Interface: MySQL 协议 + SQL
- Internal Execution: FE Vector Short Circuit + BRPC/Protobuf + BE VectorSearchExecutor
- Related Checkout-Local Drafts: `docs/vector_search_fe_bottleneck_optimization.md`、`docs/vector_search_optimization_design.md`、`docs/vector_search_framework_architecture.md`

---

## 1. 执行摘要

本设计为 StarRocks 中符合限定形态的向量 TopK SQL 增加一条类似现有点查的短路路径。客户端继续使用 MySQL/JDBC 和 SQL，但 FE 在完成语法解析、语义分析和 LogicalPlan 构建后，对可短路的向量查询执行专用规划和调度，不再进入通用 CBO、PlanFragment 生成、Fragment 下发和 Pipeline Driver 执行。

第一版的完整路径为：

```text
MySQL SQL
  -> Parser / Analyzer / LogicalPlan
  -> VectorShortCircuitPlanner
  -> Compact VectorSearchPlan
  -> FE VectorShortCircuitCoordinator
  -> BRPC exec_vector_search (fan-out by BE/CN)
  -> BE VectorSearchExecutor
  -> Segment ANN -> Tablet TopK -> BE TopK
  -> FE Global TopK
  -> MySQL TResultBatch
```

设计的核心决策如下：

1. **保留 SQL 形式**：不要求用户迁移到新的 gRPC/HTTP Search API。
2. **规划短路**：在 LogicalPlan 后识别简单向量 TopK，跳过通用 CBO memo 和无关优化规则。
3. **执行短路**：使用专用 Protobuf 请求直接调用 BE/CN，不下发 `TPlanFragment`。
4. **近数据归并**：BE/CN 完成 Segment -> Tablet -> BE 的局部 TopK，FE 只归并每个 BE 的 TopK。
5. **不复制向量语义**：普通 SQL Pipeline 和短路路径共享索引选择、过滤、delvec、exact fallback 和 refine 逻辑。
6. **线程受 WorkGroup 管理**：BRPC handler 不同步执行 ANN；向量任务进入 WorkGroup OLAP ScanExecutor，不新建脱离资源治理的线程池。
7. **严格限定并自动回退**：查询形态、谓词或输出不受支持时，无感回退原 Pipeline 路径。

---

## 2. 背景与问题

### 2.1 当前内表向量查询路径

当前内表向量查询由 FE 中的 `RewriteToVectorPlanRule` 识别 `TopN -> OlapScan`，将 ANN 参数写入 `VectorSearchOptions`，然后按普通查询生成 Fragment 和 Pipeline。

```text
SQL
  -> AST
  -> Analyze
  -> LogicalPlan
  -> Generic CBO
  -> RewriteToVectorPlanRule
  -> Physical TopN + OlapScan
  -> PlanFragmentBuilder
  -> TExecPlanFragmentParams
  -> BE FragmentExecutor
  -> Pipeline / Driver / Morsel / ScanOperator
  -> OlapChunkSource
  -> SegmentIterator ANN
  -> Pipeline TopN / Exchange / ResultSink
```

与向量检索直接相关的代码主要位于：

- [`RewriteToVectorPlanRule.java`](../../../fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/RewriteToVectorPlanRule.java)
- [`VectorSearchOptions.java`](../../../fe/fe-core/src/main/java/com/starrocks/common/VectorSearchOptions.java)
- [`PlanNodes.thrift`](../../../gensrc/thrift/PlanNodes.thrift)
- [`olap_chunk_source.cpp`](../../../be/src/exec/pipeline/scan/olap_chunk_source.cpp)
- [`segment_iterator.cpp`](../../../be/src/storage/rowset/segment_iterator.cpp)

### 2.2 通用框架引入的固定开销

对单表、单 ANN、小 K 的 serving 查询，下列工作对最终检索结果没有直接价值：

- 通用 CBO memo 和无关的变换规则。
- MV rewrite 和通用 physical property 推导。
- Fragment、Exchange、Sink 和 DescriptorTable 构建。
- Thrift `TPlanFragment` 序列化和 BE 反序列化。
- FragmentContext、Pipeline、Driver、Morsel 和 ChunkBuffer 初始化。
- 为少量 TopK 结果构建通用 TopN/Exchange/ResultSink 执行链。

这些是每次查询都要支付的固定延迟。当 ANN 索引已预热且单次索引搜索很快时，固定控制面开销可能成为端到端延迟的主要部分。

### 2.3 需要与数据面开销区分

短路无法自动消除下列数据面开销：

- 按业务 key HASH 分布导致向量查询通常需要访问所有未被分区/分桶谓词剪枝的 tablets。
- 向量索引是 per-segment 的，segment 数过多时仍要执行多次 ANN。
- 标量过滤、delvec、精确回退和量化索引 refine 仍要执行。
- 非覆盖输出列仍需要读取数据文件。

因此第一版必须分别记录控制面和数据面耗时，不能将所有向量查询延迟都归因于 SQL/Pipeline。

---

## 3. Milvus 参考与 StarRocks 映射

Milvus 对外使用面向检索的 Search/Query API，内部使用紧凑的 `VectorANNS` PlanNode，只表达 vector field、predicate、TopK、metric 和 search params，不生成通用关系物理计划。

Milvus 查询路径中值得借鉴的特性是：

1. Proxy 缓存路由信息，并发请求目标 shards。
2. QueryNode 在搜索前 pin 当前可读 segments/snapshot。
3. Segment 结果先在 QueryNode 内 reduce，再返回上层。
4. Reduce 路径中包含 heap merge 和 late materialization。
5. 兼容的小查询可按 NQ、TopK 和 deadline 进行 grouping，摊薄调度开销。
6. 需要返回宽字段时可以在候选归并后执行 requery。

StarRocks 不需要复制 Milvus 的组件拆分，对应关系如下：

| Milvus | StarRocks 短路设计 |
|---|---|
| Proxy | FE `VectorShortCircuitCoordinator` |
| Routing cache | FE tablet/replica/CN routing cache |
| Search `PlanNode` | `PVectorSearchPlan` |
| Guarantee timestamp / MVCC | physical partition visible version / GTID |
| QueryNode | BE/CN `VectorSearchExecutor` |
| Segment | Rowset Segment + vector index |
| QueryNode segment reduce | BE Segment -> Tablet -> BE TopK |
| Proxy final reduce | FE Global TopK |
| Requery | 后续版本的 `fetch_vector_search` |

参考实现：

- [Milvus architecture overview](https://milvus.io/docs/architecture_overview.md)
- [Milvus `plan.proto`](https://github.com/milvus-io/milvus/blob/master/pkg/proto/plan.proto)
- [Milvus QueryNode delegator](https://github.com/milvus-io/milvus/blob/master/internal/querynodev2/delegator/delegator.go)
- [Milvus QueryNode search task](https://github.com/milvus-io/milvus/blob/master/internal/querynodev2/tasks/search_task.go)

---

## 4. 目标与非目标

### 4.1 第一版目标

- 用户继续使用原有 MySQL/JDBC 和 SQL。
- 支持单内表或 Lake 表的单向量索引 TopK 查询。
- 支持 L2 distance 和 cosine similarity 的现有函数语义。
- 支持可下推标量谓词、距离范围谓词、delvec、refine 和 exact fallback。
- 在 LogicalPlan 后使用专用短路规划，不进入通用 CBO memo。
- 不生成 PlanFragment，不执行 Pipeline Driver。
- FE 对目标 BE/CN 执行并发 BRPC，BE/CN 只返回本节点 TopK。
- 支持 shared-nothing BE 和 shared-data CN 的路由和可见版本语义。
- 通过 WorkGroup ScanExecutor 执行 ANN，保留资源组隔离和公平性。
- 不支持的查询形态自动回退原 Pipeline。

### 4.2 第一版非目标

- 不新增对外 gRPC/HTTP Search API。
- 不支持 JOIN、聚合、window、subquery、CTE 和任意 UDF。
- 不支持多向量字段联合检索和 hybrid fusion。
- 不支持无界 range search 流式返回。
- 不实现跨查询 NQ grouping/micro-batching。
- 不实现二阶段 candidate fetch/requery。
- 不引入 vector-aware tablet 路由或全局 coarse vector index。
- 不试图替换通用 SQL 向量执行路径。

---

## 5. 短路查询语义和准入条件

### 5.1 支持的典型 SQL

```sql
SELECT id,
       title,
       approx_l2_distance(embedding, [0.1, 0.2, 0.3]) AS score
FROM documents
WHERE tenant_id = 1001
  AND category IN ('database', 'ai')
ORDER BY score ASC
LIMIT 10;
```

Cosine similarity 使用降序：

```sql
SELECT id,
       approx_cosine_similarity(embedding, [0.1, 0.2, 0.3]) AS score
FROM documents
ORDER BY score DESC
LIMIT 10;
```

### 5.2 逻辑计划形态

短路检测应允许 projection/alias 的等价形态，但最终必须约化为：

```text
LogicalTopN
  orderKeys = [vector_distance]
  limit > 0
  offset >= 0
    -> LogicalOlapScan
         projection = base columns + same vector_distance expression
         predicate  = supported scalar/range predicates
```

### 5.3 准入条件

只有全部满足时才可进入短路：

1. 整个查询只有一个 OLAP/Lake table scan。
2. 存在与距离函数字段、metric 和维度匹配的 vector index。
3. TopN 只有一个排序键，且该键是受支持的向量距离/相似度函数。
4. `limit + offset` 不超过系统安全上限。
5. Projection 只包含基础列、受支持的 cast 和与排序键相同的距离表达式。
6. 所有谓词都能转换为 storage `PredicateTree` 或向量 range。
7. 不包含 JOIN、聚合、window、distinct、set operation、subquery 或 CTE。
8. 不包含需要上层 operator 执行的 runtime filter 或 non-pushdown filter。
9. 输出协议为 MySQL text row 或 prepared statement binary row。
10. 目标 BE/CN 都报告支持当前 vector short-circuit protocol version。

### 5.4 回退原则

- `AUTO` 模式：任何准入条件不满足，返回普通规划。
- `FORCE` 模式：不满足时返回明确的 `UNSUPPORTED_VECTOR_SHORT_CIRCUIT` 错误，用于测试和定位。
- 查询已在部分 BE 上执行后不应默默重跑整个 Pipeline，避免超时放大和不可见的重复开销。
- 协议能力不足、schema 已过期等发生在任何远程执行之前的错误，可以安全回退或重新规划一次。

---

## 6. 端到端架构

```text
Client
  | MySQL SQL
  v
FE StmtExecutor
  | Parser / Analyzer / RelationTransformer
  v
VectorShortCircuitPlanner
  |-- not eligible --> Generic Optimizer / Fragment / Pipeline
  |
  `-- eligible
        |
        v
  VectorShortCircuitPlan
        |
        v
  VectorShortCircuitCoordinator
        | capture partition/tablet versions
        | route tablets to BE/CN
        | dispatch all RPCs concurrently
        |
        +--------------+--------------+
        v              v              v
      BE/CN 1        BE/CN 2        BE/CN N
        |              |              |
        | WorkGroup ScanExecutor      |
        | Tablet/Segment ANN           |
        | BE-local TopK                |
        +--------------+--------------+
                       |
                       v
                FE Global TopK
                       |
                       v
                MySQL TResultBatch
```

复杂度目标：

```text
BE -> FE candidate count <= num_BEs * (limit + offset)
FE merge = O(num_BEs * local_k * log(num_BEs)) per query
```

不允许 BE 向 FE 返回 per-segment 结果。

---

## 7. FE 设计

### 7.1 组件划分

新增包：

```text
fe/fe-core/src/main/java/com/starrocks/qe/vector/
  VectorSearchSpec.java
  VectorSearchSpecExtractor.java
  VectorShortCircuitPlanner.java
  VectorShortCircuitPlan.java
  VectorSearchRoutingProvider.java
  VectorShortCircuitCoordinator.java
  VectorSearchReducer.java
  VectorShortCircuitProfile.java
```

职责边界：

| 组件 | 职责 |
|---|---|
| `VectorSearchSpecExtractor` | 从 LogicalPlan 中提取并验证向量检索语义 |
| `VectorShortCircuitPlanner` | 检查准入条件，执行必要的列/分区/分桶剪枝 |
| `VectorShortCircuitPlan` | 保存稳定的索引、列、谓词、TopK、输出和协议信息 |
| `VectorSearchRoutingProvider` | 捕获 visible version，选择 replica/CN，按目标节点分组 tablets |
| `VectorShortCircuitCoordinator` | admission、并发 RPC、重试、取消、全局归并和结果返回 |
| `VectorSearchReducer` | 对已排序的 BE TopK 执行 k-way merge |

### 7.2 与 StatementPlanner 的集成

接入点位于 [`StatementPlanner.java`](../../../fe/fe-core/src/main/java/com/starrocks/sql/StatementPlanner.java) 完成 LogicalPlan 构建之后、创建通用 `Optimizer` 之前。

概念代码：

```java
LogicalPlan logicalPlan = relationTransformer.transformWithSelectLimit(query);

Optional<VectorShortCircuitPlan> vectorPlan =
        VectorShortCircuitPlanner.tryBuild(logicalPlan, session);

if (vectorPlan.isPresent()) {
    return ExecPlan.forVectorShortCircuit(
            session,
            logicalPlan,
            vectorPlan.get(),
            resultSinkType);
}

// Existing generic optimizer path.
OptExpression optimizedPlan = optimizer.optimize(...);
```

### 7.3 复用 VectorSearchSpecExtractor

当前 [`RewriteToVectorPlanRule.java`](../../../fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/RewriteToVectorPlanRule.java) 包含以下逻辑：

- 识别 distance/similarity 函数和输入列。
- 匹配 vector index。
- 检查 query vector 维度。
- 处理 ascending/descending 方向。
- 分离 vector range 和 residual scalar predicate。
- 决定是否 refine。
- 生成距离虚拟列。

不应在短路中复制这些逻辑。第一版需要先将其中的纯语义提取部分移到 `VectorSearchSpecExtractor`，然后由两条路径共用：

```text
Generic CBO RewriteToVectorPlanRule --+
                                      +--> VectorSearchSpecExtractor
VectorShortCircuitPlanner ------------+
```

### 7.4 ExecPlan 表示

第一版不引入全新的 statement plan 类型体系，而是为 `ExecPlan` 新增可选短路计划：

```java
private VectorShortCircuitPlan vectorShortCircuitPlan;

public boolean isVectorShortCircuit();
public VectorShortCircuitPlan getVectorShortCircuitPlan();
```

当 `isVectorShortCircuit()` 为 `true` 时：

- fragments 和 scan nodes 为空。
- 不调用 `PlanFragmentBuilder.createPhysicalPlan()`。
- EXPLAIN 使用 `VectorShortCircuitPlan` 的专用打印方法。
- StmtExecutor/Coordinator factory 构造 `VectorShortCircuitCoordinator`。

如果 `ExecPlan` 的现有不变式使得空 fragments 改动面过大，可以引入一个只包含 metadata 的伪 fragment 作为过渡，但不应将它序列化或下发到 BE。

### 7.5 分区、分桶和路由

不应为短路创建假 `OlapScanNode`。需要从 [`OlapScanNode.java`](../../../fe/fe-core/src/main/java/com/starrocks/planner/OlapScanNode.java) 抽取可共享的 read routing provider，包含：

- range/list partition pruning。
- hash/range distribution pruning。
- physical partition visible version 捕获。
- schema hash 捕获。
- shared-nothing queryable replica 选择。
- shared-data warehouse 和 CN 分配。
- tablet affinity、page cache 和 disk cache 选项。
- replica/CN 备选路由。

路由结果必须按 BE/CN 分组：

```text
Node A -> [tablet 1@v10, tablet 4@v20, tablet 7@v31]
Node B -> [tablet 2@v10, tablet 5@v20, tablet 8@v31]
Node C -> [tablet 3@v10, tablet 6@v20, tablet 9@v31]
```

正常情况下每个节点一个 RPC；如果单请求超过 BRPC/protobuf 安全大小，再按目标列表大小拆分，不按 tablet 固定拆分。

### 7.6 并发下发与重试

FE 必须先发出所有 RPC，再等待结果：

```java
List<NodeRequest> requests = routing.groupByNode();
List<Future<PExecVectorSearchResult>> futures = dispatchAll(requests);
List<NodeResult> results = awaitAllUntilDeadline(futures);
```

不应复用当前点查 `ShortCircuitHybridExecutor` 中一边遍历一边 `future.get()` 的同步模式。

重试规则：

- 请求和响应都携带 tablet IDs。
- BE/CN 对每个 tablet 返回完成状态。
- FE 只对缺失/失败 tablets 选择备选 replica/CN 重试。
- 重试使用原 visible version 和 schema hash。
- 响应按 tablet ID 去重，防止超时竞态产生双份 partial result。
- 不在 deadline 之后启动新的 replica 重试。

### 7.7 FE Global TopK

每个 BE/CN 返回按 score 排序的 `local_k = limit + offset` 结果。FE 执行 k-way merge：

```text
L2 distance       -> score ascending
cosine similarity -> score descending
```

同分必须使用确定性 tie breaker：

```text
(score, tablet_id, segment_id, row_id)
```

归并后：

1. 保留前 `limit + offset` 个候选。
2. 丢弃前 `offset` 个。
3. 将剩余结果的已编码 MySQL rows 写入最终 `TResultBatch`。

---

## 8. 内部 Protobuf 和 RPC

### 8.1 协议文件

新增：

```text
gensrc/proto/vector_search.proto
```

修改：

```text
gensrc/proto/internal_service.proto
```

遵守仓库协议规则：

- 字段使用 `optional`/`repeated`，不新增 `required`。
- 字段序号只增不复用。
- 不手工修改生成文件。

### 8.2 查询计划

```protobuf
syntax = "proto2";

package starrocks;

message PVectorSearchPlan {
    optional uint32 protocol_version = 1;

    optional int64 table_id = 2;
    optional int64 index_id = 3;
    optional int64 schema_id = 4;
    optional int32 vector_column_uid = 5;

    optional uint32 metric_type = 6;
    optional uint32 result_order = 7;
    optional uint32 dim = 8;

    // Little-endian IEEE-754 float32 values. V1 contains one query vector.
    optional bytes query_vector_f32 = 9;

    optional uint64 top_k = 10;
    optional uint64 offset = 11;
    optional uint64 candidate_k = 12;

    optional double vector_range = 13;
    optional bool refine = 14;

    optional PVectorPredicate predicate = 15;
    repeated int32 output_column_uids = 16;
    repeated PVectorSearchParam search_params = 17;

    optional bool mysql_binary_row = 18;
}
```

V1 使用单 query vector，但线上格式使用 binary，避免当前 `list<string> -> std::stof` 的开销。后续增加 `nq` 和批量 buffer 时不需要修改基本语义。

### 8.3 谓词

```protobuf
message PVectorPredicate {
    oneof node {
        PVectorCompoundPredicate compound = 1;
        PVectorBinaryPredicate binary = 2;
        PVectorInPredicate in_predicate = 3;
        PVectorIsNullPredicate is_null = 4;
        PVectorMatchPredicate match = 5;
    }
}
```

V1 支持：

- `AND` / `OR`
- `=` / `!=` / `<` / `<=` / `>` / `>=`
- `IN` / `NOT IN`
- `IS NULL` / `IS NOT NULL`
- 已有 storage index 可支持的 `MATCH`

列使用 stable column UID，literal 在 FE 完成类型检查和强制转换，BE 不重新执行 SQL 语义分析。

### 8.4 执行目标和请求

```protobuf
message PVectorSearchTarget {
    optional int64 tablet_id = 1;
    optional int64 partition_id = 2;
    optional int64 version = 3;
    optional int32 schema_hash = 4;
}

message PExecVectorSearchRequest {
    optional PUniqueId query_id = 1;
    optional int64 deadline_ms = 2;
    optional int64 resource_group_id = 3;
    optional int64 warehouse_id = 4;

    optional PVectorSearchPlan plan = 5;
    repeated PVectorSearchTarget targets = 6;
}
```

### 8.5 局部结果

为了避免 FE 解析 BE `ChunkPB` 或重新执行类型编码，V1 由 BE 将每个 candidate 编码为 MySQL row，同时将 score 和 tie-breaker 作为独立字段返回：

```protobuf
message PVectorSearchCandidate {
    optional float score = 1;
    optional int64 tablet_id = 2;
    optional int64 segment_id = 3;
    optional uint32 row_id = 4;

    // Encoded as MySQL text or binary row according to the request.
    optional bytes mysql_row = 5;
}

message PVectorTabletStatus {
    optional int64 tablet_id = 1;
    optional StatusPB status = 2;
}

message PExecVectorSearchResult {
    optional StatusPB status = 1;
    repeated PVectorSearchCandidate candidates = 2;
    repeated PVectorTabletStatus tablet_statuses = 3;
    optional PVectorSearchProfile profile = 4;
}
```

响应中的 candidates 是 BE 级 TopK，已按 score 排序。

### 8.6 RPC

在 `PInternalService` 新增：

```protobuf
rpc exec_vector_search(PExecVectorSearchRequest)
    returns (PExecVectorSearchResult);

rpc cancel_vector_search(PCancelVectorSearchRequest)
    returns (PCancelVectorSearchResult);
```

`fetch_vector_search` 属于二阶段 late materialization，不在 V1 实现，但应为协议预留新 RPC 扩展空间。

---

## 9. BE/CN 组件设计

### 9.1 文件划分

新增：

```text
be/src/exec/vector_search/
  vector_search_executor.h
  vector_search_executor.cpp
  vector_search_context.h
  vector_search_context.cpp
  vector_search_context_manager.h
  vector_search_context_manager.cpp
  vector_search_task.h
  vector_search_task.cpp
  vector_search_reducer.h
  vector_search_reducer.cpp
  vector_result_writer.h
  vector_result_writer.cpp
```

职责：

| 组件 | 职责 |
|---|---|
| `VectorSearchExecutor` | 入口校验、创建 context、提交任务、完成响应 |
| `VectorSearchContext` | 查询状态、deadline、cancel、snapshot、work cursor、lane heaps |
| `VectorSearchContextManager` | 按 query ID 管理运行中任务，支持 cancel 和超时清理 |
| `VectorSearchTask` | WorkGroup ScanExecutor 中的有界 lane 任务 |
| `VectorSearchReducer` | tablet/lane/BE 层 TopK 归并 |
| `VectorResultWriter` | 按 MySQL text/binary row 协议编码最终候选 |

### 9.2 V1 复用 TabletReader/SegmentIterator

V1 以不复制存储语义为优先级，重用：

- [`TabletReader`](../../../be/src/storage/tablet_reader.cpp)
- [`SegmentIterator`](../../../be/src/storage/rowset/segment_iterator.cpp)
- [`VectorIndexReader`](../../../be/src/storage/index/vector/vector_index_reader.h)
- [`TenANNReader`](../../../be/src/storage/index/vector/tenann_index_reader.cpp)

每个 tablet task：

```text
capture tablet/version snapshot
  -> build TabletReaderParams
  -> use_vector_index = true
  -> build PredicateTree
  -> set VectorSearchOption
  -> TabletReader.prepare/open
  -> drain candidate chunks
  -> tablet-local TopK heap
```

这可以自然复用：

- vector index cache。
- scalar index 和 predicate evaluation。
- delvec/delete predicate。
- missing index 的 exact fallback。
- filtered ANN 和 selectivity gate。
- quantized index refine。
- 虚拟 distance column。

V1 的已知局限是 TabletReader 会在 per-segment 候选上读取 projection 列，之后才进入 BE TopK，尚未实现 candidate-first late materialization。

### 9.3 后续抽取 SegmentVectorSearcher

V2 将 [`SegmentIterator::_get_row_ranges_by_vector_index`](../../../be/src/storage/rowset/segment_iterator.cpp) 抽取成：

```cpp
struct SegmentVectorSearchResult {
    std::vector<rowid_t> rowids;
    std::vector<float> scores;
    bool used_ann = false;
    bool used_exact = false;
    bool refined = false;
};

class SegmentVectorSearcher {
public:
    Status search(const SegmentVectorSearchRequest& request,
                  SegmentVectorSearchResult* result);
};
```

然后由普通 SegmentIterator 和短路 VectorSearchExecutor 共用。这是实现 segment 级公平调度和 late materialization 的前置条件，不属于 V1 的强制项。

---

## 10. BE/CN 线程模型

### 10.1 设计结论

```text
BRPC bthread
  -> only validate/copy/register context
  -> submit asynchronous work
  -> return without waiting

WorkGroup OLAP ScanExecutor
  -> bounded vector search lanes
  -> tablet work items in V1
  -> synchronous single-thread ANN call per lane
  -> lane-local TopK
  -> BE final TopK
  -> response + done->Run()
```

不应完全照搬当前点查的线程模型。现有 [`exec_short_circuit`](../../../be/src/service/internal_service.cpp) 在 RPC handler 中同步执行 `prepare -> execute -> fetch_data`，对微秒/小毫秒级 PK lookup 可以接受，但不适合可能访问大量 tablets/segments 的 ANN。

### 10.2 RPC handler

RPC handler 只允许：

- 校验 protocol version 和必要字段。
- 验证 query vector buffer 长度。
- 复制请求到 `VectorSearchContext`。
- 解析 WorkGroup/resource group。
- 注册 query ID。
- 向 WorkGroup ScanExecutor 提交初始任务。

RPC handler 不允许：

- 同步加载向量索引。
- 同步读取 tablet/segment 数据。
- 执行 ANN。
- 等待 ScanExecutor future。
- 执行 TopK merge 或 MySQL row 编码。

概念代码：

```cpp
void PInternalServiceImpl::exec_vector_search(
        RpcController* controller,
        const PExecVectorSearchRequest* request,
        PExecVectorSearchResult* response,
        Closure* done) {
    auto context = vector_search_context_mgr()->create(
            copy_request(*request), controller, response, done);

    Status status = vector_search_executor()->submit(context);
    if (!status.ok()) {
        status.to_protobuf(response->mutable_status());
        vector_search_context_mgr()->remove(context->query_id());
        done->Run();
    }
}
```

`done` 的所有权在提交成功后转移给 context，最后一个完成任务负责调用 `done->Run()`。

### 10.3 WorkGroup ScanExecutor

向量短路不经过 Pipeline DriverExecutor，但必须使用目标 WorkGroup 的 OLAP ScanExecutor：

```cpp
auto* scan_executor = workgroup->executors()->scan_executor();
scan_executor->submit(std::move(task));
```

不应默认使用 shared/common executor，否则 exclusive WorkGroup 的 CPU 绑定和隔离将失效。

现有 [`ScanExecutor`](../../../be/src/compute_env/workgroup/scan_executor.cpp) 提供：

- 与 CPU cores 对齐的物理 worker threads。
- WorkGroup 二级队列和 vruntime 公平调度。
- exclusive CPU 和 CPU borrowing。
- 有界队列与 backpressure。
- task runtime 统计。
- 未完成 `ScanTask` 的自动重新入队。

因此 V1 不新建独立 `vector_search_thread_pool`。如果后续需要隔离普通 OLAP scan 和 ANN，应实现共享 WorkGroup schedule policy/CPU quota 的 `VectorScanExecutor`，而不是无资源约束的新线程池。

### 10.4 V1 的并行单位

V1 使用 **tablet work item**，每个调度 lane 一次处理一个 tablet：

```text
Lane 0: tablet 1 -> yield -> tablet 5 -> yield -> ...
Lane 1: tablet 2 -> yield -> tablet 6 -> yield -> ...
Lane 2: tablet 3 -> yield -> tablet 7 -> yield -> ...
Lane 3: tablet 4 -> yield -> tablet 8 -> yield -> ...
```

Context 保存共享 work cursor：

```cpp
std::vector<TabletWorkItem> work_items;
std::atomic<size_t> next_work_item{0};
```

并行 lane 数：

```text
lanes = min(
    work_items.size,
    request_max_parallelism,
    vector_search_max_parallelism_per_query,
    workgroup_scan_parallelism
)
```

具体默认并行度在 benchmark 后确定，不在设计阶段假定固定数值。

不应将所有 tablets/segments 全部作为独立 task 一次性塞入 ScanExecutor 队列；队列中任务数应约等于：

```text
active_vector_queries * lanes_per_query
```

每个 lane 通过 atomic cursor 领取下一个 tablet。

### 10.5 Yield 和公平性

V1 在 tablet 边界 yield。每处理完一个 tablet 后：

1. 检查 cancel/deadline。
2. 检查 WorkGroup 是否应让出 CPU。
3. 达到时间片或存在更优先 WorkGroup 时，保留 continuation，让 `ScanExecutor` 自动将任务重新入队。

V1 的局限是：一个 tablet 包含较多 segments 时，TabletReader 内部可能连续执行多个 ANN，无法在 segment 间被 WorkGroup 完全感知。V2 抽取 `SegmentVectorSearcher` 后，并行/yield 单位改为 segment。

### 10.6 ANN 内部线程

当前 [`TenANNReader::search`](../../../be/src/storage/index/vector/tenann_index_reader.cpp) 以同步方式调用 `AnnSearch`。V1 将一次 ANN 视为占用一个 ScanExecutor worker 的同步操作。

默认并行原则：

```text
ANN internal threads = 1
parallelism comes from tablets/segments/queries
```

如果 TenANN/FAISS 的特定索引内部使用 OpenMP 或其他线程池，必须满足：

```text
outer_lanes * ann_internal_threads <= workgroup CPU quota
```

优先关闭单次 ANN 的内部多线程，避免外层 tablet 并行和内层 ANN 并行叠加。

### 10.7 局部 TopK 减少锁竞争

每个 lane 拥有独立的 TopK heap：

```text
Lane 0 -> LocalTopK[0]
Lane 1 -> LocalTopK[1]
Lane 2 -> LocalTopK[2]
Lane 3 -> LocalTopK[3]
```

不在每个 candidate 产生时写入全局共享 heap。最后一个 lane 完成时：

```text
merge lane heaps
  -> BE TopK
  -> VectorResultWriter
  -> populate response
  -> unregister context
  -> done->Run()
```

因为 K 较小，最终 heap merge 不需要独立 reduce 线程池。

### 10.8 Index cache miss

当前 `TenANNReader::init_searcher()` 的 index cache miss 会在调用线程同步加载索引。V1 与普通 Pipeline 保持一致，允许 ScanExecutor worker 在 cache miss 时同步加载，但必须记录：

- cache hit/miss。
- index load wait time。
- local/remote bytes。
- 同一 index 的 concurrent load 是否被 cache single-flight 合并。

如果 shared-data 冷索引加载导致 ScanExecutor 长时间阻塞，V2 引入只负责 index/object-storage I/O 的 `VectorIndexLoadExecutor`，加载完成后将 search continuation 重新投递到 WorkGroup ScanExecutor。ANN CPU 仍不进入 load executor。

---

## 11. BE 执行状态机

```text
RECEIVED
  -> VALIDATING
  -> REGISTERED
  -> PREPARING_SNAPSHOTS
  -> SEARCHING_TABLETS
  -> REDUCING
  -> ENCODING_ROWS
  -> FINISHED

Any state
  -> CANCELLED
  -> FAILED
  -> TIMED_OUT
```

`VectorSearchContext` 至少包含：

```cpp
class VectorSearchContext {
public:
    TUniqueId query_id;
    int64_t deadline_ns;

    std::atomic<bool> cancelled{false};
    std::atomic<size_t> next_work_item{0};
    std::atomic<int32_t> running_lanes{0};

    std::vector<TabletWorkItem> work_items;
    std::vector<LocalTopKHeap> lane_heaps;

    std::mutex status_mutex;
    Status first_error;

    WorkGroupPtr workgroup;
    std::shared_ptr<MemTracker> query_mem_tracker;

    brpc::Controller* controller;
    PExecVectorSearchResult* response;
    google::protobuf::Closure* done;
};
```

并发错误处理使用 first-error-wins，其他 lanes 观察到失败/cancel 后尽快停止领取新 tablet。

检查 cancel/deadline 的位置：

- 获取 tablet work item 前。
- snapshot 捕获后。
- predicate/index 初始化后。
- ANN 调用前后。
- 读取候选 chunk 的循环中。
- final reduce 前。
- MySQL row 编码前。

单次 HNSW/IVF 调用暂时不可抢占，因此最坏 cancel latency 是一次 segment ANN 的时间。

---

## 12. Snapshot、版本与一致性

### 12.1 FE 捕获版本

FE 在生成路由时为每个 physical partition/tablet 捕获：

- visible version。
- schema hash/schema ID。
- 可选 GTID。
- 备选 replica/CN。

这些值在整个查询和 replica 重试中保持不变。

### 12.2 BE 捕获 rowsets

BE 对每个 tablet target 按 version 捕获 consistent rowsets，并在对应 tablet task 完成前保持 reader/reference，确保：

- compaction 不会使正在使用的 rowsets 失效。
- PK delvec 与请求版本一致。
- vector index 和 projection columns 来自同一 snapshot。
- replica retry 不会混合不同 visible version 的结果。

### 12.3 Schema 变更

BE 检查：

- request schema ID/hash 与 tablet schema 一致。
- vector column UID 仍存在。
- vector index ID 仍存在且关联同一 column UID。
- output column UIDs 仍存在且类型未发生不兼容变更。

不匹配返回 `STALE_VECTOR_SEARCH_PLAN`。FE 可以在未产生部分结果时重新 analyze/route 一次。

---

## 13. 过滤、ANN 与 Refine

### 13.1 谓词处理

FE 将受支持的 scalar predicate 编译为 `PVectorPredicate`，BE 根据 tablet schema 构造 storage `PredicateTree`。

每个 segment 的决策逻辑与普通 Pipeline 保持一致：

```text
matched rows == 0
  -> skip segment

matched rows <= candidate_k
  -> exact distance over matched rows

index supports filtered search
  -> pre-filter ANN

index/filter combination cannot guarantee enough rows
  -> existing exact fallback
```

### 13.2 local_k 和 candidate_k

定义：

```text
global_k = limit + offset
local_k = global_k
candidate_k = local_k * k_factor
```

`candidate_k` 用于 segment ANN/refine，BE 对所有 tablets 归并后只返回 `local_k`。

对量化索引：

```text
ANN candidate_k * refine_factor
  -> read full-precision vectors
  -> exact score
  -> tablet/BE local_k
```

### 13.3 正确性

对确定性分片搜索，每个 BE 返回本地前 `global_k` 足以构造全局前 `global_k`。ANN 本身的 approximate recall 由 index/search params 决定，短路不应修改与普通 Pipeline 相同的参数语义。

---

## 14. 结果物化和 MySQL 输出

### 14.1 V1 单阶段物化

V1 每个 tablet/segment 仍通过 TabletReader/SegmentIterator 读取 projection columns，候选进入 lane-local heap 时将所需行复制到紧凑 candidate chunk。

```text
Segment ANN
  -> candidate rowids/scores
  -> read projected columns
  -> lane-local heap
  -> BE TopK
  -> MySQL row encoding
```

只对 BE TopK 执行最终 MySQL row 编码，不对每个 segment candidate 提前编码。

### 14.2 VectorResultWriter

`VectorResultWriter` 可复用当前 `MysqlResultMemorySink` 中的 `MysqlRowBuffer` 逻辑，但 V1 的 projection 是基础列 + 可选 distance pseudo-column，不需要下发通用 `TExpr` 和 `TDescriptorTable`。

BE 按请求中的 `mysql_binary_row` 选择 text/binary row 编码。FE 只根据 score/tie-breaker 选择已编码 rows，不解析字段内容。

### 14.3 V2 late materialization

V2 在 BE 内先归并 row locator/score，只为 BE TopK 读取 projection columns。输出非常宽时，可进一步实现 FE global TopK 后的 `fetch_vector_search`。V1 不引入跨 RPC pinned context，降低首版实现和资源回收复杂度。

---

## 15. Admission、Backpressure 与内存

### 15.1 FE admission

`VectorShortCircuitCoordinator` 必须进入与普通查询等价的 QueryQueue/slot/resource group 流程，不应因短路而绕过查询限流。

资源估算至少考虑：

- 目标 BE/CN 数。
- tablet/segment 数。
- query vector 维度。
- `candidate_k`。
- 预计 projection row size。
- 是否 refine。

### 15.2 BE backpressure

- ScanExecutor `submit()` 失败时快速返回 `SERVICE_UNAVAILABLE`。
- 每个查询有限的 lane 数和 candidate memory。
- 运行中的 vector contexts 有全局上限。
- 每个 WorkGroup 通过已有 CPU weight 和 memory limit 受控。
- 超过安全上限的 `K`、`candidate_k`、vector dimension 或 projection size 在 FE/BE 双重拒绝。

### 15.3 内存跟踪

Query-owned 内存计入 query/workgroup tracker：

- request 和 query vector buffer。
- predicate tree。
- tablet/snapshot handles。
- candidate chunks。
- lane-local heaps。
- MySQL row buffers。
- protobuf response。

Vector index cache 继续由全局 vector index cache/process tracker 管理，不将共享索引字节重复计入查询 memory limit。

ScanExecutor worker 执行任务时必须安装正确的 thread-local query mem tracker。

---

## 16. 取消、超时和故障处理

### 16.1 取消

FE 在客户端断开、KILL QUERY、deadline 超时或某个必要节点失败时，向所有已下发节点发送 `cancel_vector_search(query_id)`。

BE `VectorSearchContextManager` 将 context 标记为 cancelled，正在执行的 ANN 在返回后观察状态，未开始的 tablets 不再领取。

### 16.2 部分失败

V1 默认不返回 partial TopK。任何 tablet 在重试后仍失败，整个查询失败。

可观测响应中保留 tablet-level status，但不向 MySQL 客户端返回静默缺数据的成功结果。

### 16.3 滚动升级

- BE/CN heartbeat 中报告 vector short-circuit protocol capability/version。
- `AUTO` 模式下只有当所有目标节点支持当前协议时才进入短路。
- 升级窗口中不支持的查询走原 Pipeline。
- 新 Protobuf 字段只使用 optional/repeated，旧 BE 不会因未知字段崩溃。

---

## 17. 可观测性

### 17.1 FE Profile

```text
VectorShortCircuit
  EligibleCheckTime
  PlanBuildTime
  PartitionPruneTime
  TabletRouteTime
  AdmissionWaitTime
  RpcDispatchTime
  RpcWaitTime
  RetryCount
  NodesContacted
  TabletsTargeted
  CandidatesReceived
  GlobalReduceTime
  ResultBuildTime
```

EXPLAIN 输出示例：

```text
VECTOR SHORT CIRCUIT: ON
  Table: documents
  Index: idx_embedding
  Metric: L2_DISTANCE
  Limit: 10
  Offset: 0
  CandidateK: 40
  Refine: OFF
  Predicate: tenant_id = 1001
  Output: [id, title, score]
```

不满足准入条件时，trace 记录一个结构化 reject reason，例如：

```text
VECTOR SHORT CIRCUIT: OFF
  Reason: NON_PUSHDOWN_PREDICATE
```

### 17.2 BE Profile

```text
VectorSearchExecutor
  RpcDeserializeTime
  QueueWaitTime
  ContextPrepareTime
  SnapshotCaptureTime
  TabletsTotal
  TabletsFinished
  SegmentsTotal
  SegmentsPruned
  VectorIndexCacheHit
  VectorIndexCacheMiss
  VectorIndexLoadTime
  PredicateEvaluateTime
  VectorSearchTime
  ExactFallbackTime
  RefineTime
  CandidateRows
  TabletReduceTime
  BEReduceTime
  MaterializeTime
  MysqlEncodeTime
  ResponseSerializeTime
  Cancelled
```

线程池维度记录：

- pending/running/finished vector tasks。
- active vector contexts。
- lanes per query。
- queue reject count。
- WorkGroup 维度的 vector CPU time。
- 单次 tablet/segment ANN 的 p50/p95/p99。

### 17.3 Audit

审计日志继续记录原 SQL、用户、数据库、资源组和 query ID，并额外记录：

- `execution_mode=VECTOR_SHORT_CIRCUIT`
- vector index ID/name。
- dimension、K、candidateK、refine。
- target nodes/tablets/segments。
- fallback/retry reason。

Query vector 本身不重复记录到额外日志字段，避免日志放大和敏感数据暴露。

### 17.4 低开销阶段耗时采集

现有 SQL/Pipeline 与后续 short-circuit 的性能归因统一遵循以下原则：

- 只在诊断样本上开启 `enable_profile=true`；正式吞吐和延迟结果使用 Profile 关闭的查询，另做一次 Profile on/off A/B 校验观测开销。
- FE 复用 `Tracers.watchScope` 已有边界，包括 `Parser`、`Analyzer`、`Transformer`、`Optimizer`、`ExecPlanBuild`、`DeploySerializeConcurrencyTime`、`DeployStageByStageTime`、`DeployAsyncSendTime` 和 `DeployWaitTime`。计时仍由已有 `Stopwatch` 完成，只在生成 Profile 文本时把纳秒值格式化为 `ns/us/ms`，不增加查询日志。
- BE 复用 `SegmentIterator` 已有的 `SCOPED_RAW_TIMER`，ANN 热循环不增加第二套时钟。shared-data `LakeDataSource` 仅在 `_use_vector_index=true` 且该查询开启 Profile 时创建并汇总 `GetVectorRowRangesTime`、`VectorSearchTime`、`ProcessVectorDistanceAndIdTime` 和 `VectorIndexFilterRows`；Profile off 不创建或更新这些新增 counters。
- 标量回捞使用现有 `FETCH_SINK/FETCH_SOURCE`、`GenFetchTasksTime`、`NetworkTime`、`SerializeTime`、`DeserializeTime`、`SegmentRead` 和 `BlockFetch`，不增加逐行计时或日志。
- 原始 Profile 以 query ID 单独落盘；日志中不输出 query vector，也不在高 QPS sweep 中逐查询打印结构化明细。

阶段口径如下：

| 目标阶段 | Profile 口径 |
|---|---|
| SQL 到 AST | `Parser`（文本 SQL）；prepared execute 不重复计入 prepare 阶段解析 |
| 语义分析 | `Analyzer` |
| 逻辑计划生成 | `Transformer` |
| CBO | `Optimizer`，并细分 `RuleBaseOptimize`、`CostBaseOptimize`、`PhysicalRewrite` |
| PlanFragment 构建 | `ExecPlanBuild` |
| Fragment 序列化 | `DeploySerializeConcurrencyTime` |
| RPC 发起 | `DeployStageByStageTime`、`DeployAsyncSendTime` |
| RPC/BE fragment 初始化确认 | `DeployWaitTime` |
| ANN | `VectorSearchTime` |
| ANN 结果 ID/距离处理 | `ProcessVectorDistanceAndIdTime` |
| 标量回捞 | `FETCH_*` 与 `SegmentRead/BlockFetch` |

---

## 18. 配置与会话开关

建议的 FE session variables：

| 变量 | 语义 |
|---|---|
| `vector_short_circuit_mode` | `OFF` / `AUTO` / `FORCE` |
| `vector_short_circuit_max_k` | 短路允许的 `limit + offset` 上限 |
| `vector_short_circuit_max_parallelism` | 每个 BE/CN 请求的最大 lane 并行度 |

建议的 BE configs：

| 变量 | 语义 |
|---|---|
| `vector_search_max_parallelism_per_query` | 单查询在一个 BE/CN 上的最大 lanes |
| `vector_search_max_active_contexts` | BE/CN 同时运行的 vector contexts 上限 |
| `vector_search_max_pending_tasks` | 向量短路的安全队列上限 |
| `vector_search_context_timeout_ms` | context 超时清理上限 |

具体默认值需要通过不同 CPU、segment 数和索引类型的 benchmark 确定。实现任何用户可见配置时，必须同步更新公开配置文档。

---

## 19. 代码改造清单

### 19.1 FE 新增

| 文件 | 说明 |
|---|---|
| `fe/.../qe/vector/VectorSearchSpec.java` | 与执行框架无关的向量查询语义 |
| `fe/.../qe/vector/VectorSearchSpecExtractor.java` | 从 LogicalPlan 提取和验证 spec |
| `fe/.../qe/vector/VectorShortCircuitPlanner.java` | 短路准入与专用规划 |
| `fe/.../qe/vector/VectorShortCircuitPlan.java` | 短路执行计划 |
| `fe/.../qe/vector/VectorSearchRoutingProvider.java` | partition/tablet/replica/CN 路由 |
| `fe/.../qe/vector/VectorShortCircuitCoordinator.java` | admission、RPC、retry、cancel、result queue |
| `fe/.../qe/vector/VectorSearchReducer.java` | FE global TopK merge |
| `fe/.../qe/vector/VectorShortCircuitProfile.java` | FE profile |

### 19.2 FE 修改

| 文件 | 改造 |
|---|---|
| `StatementPlanner.java` | LogicalPlan 后尝试 vector short circuit |
| `RewriteToVectorPlanRule.java` | 复用 `VectorSearchSpecExtractor` |
| `ExecPlan.java` | 支持 optional vector short-circuit plan |
| `StmtExecutor.java` | 为短路计划创建专用 coordinator |
| `DefaultCoordinator.java`/工厂 | 增加 vector coordinator 选路，或抽取共用 coordinator factory |
| `PBackendService.java` | `execVectorSearch`/`cancelVectorSearch` BRPC |
| `PBackendServiceWithMetrics.java` | RPC metrics |
| `SessionVariable.java` | 短路模式和安全上限 |
| `OlapScanNode.java` 及其路由依赖 | 抽取共享 routing provider |

### 19.3 协议

| 文件 | 改造 |
|---|---|
| `gensrc/proto/vector_search.proto` | 新增 compact plan/request/result |
| `gensrc/proto/internal_service.proto` | 新增 exec/cancel RPC |

### 19.4 BE 新增

| 文件 | 说明 |
|---|---|
| `be/src/exec/vector_search/vector_search_executor.*` | 专用执行入口 |
| `be/src/exec/vector_search/vector_search_context.*` | 查询状态与 async RPC lifetime |
| `be/src/exec/vector_search/vector_search_context_manager.*` | context 注册/cancel/timeout |
| `be/src/exec/vector_search/vector_search_task.*` | WorkGroup ScanTask lanes |
| `be/src/exec/vector_search/vector_search_reducer.*` | tablet/lane/BE TopK |
| `be/src/exec/vector_search/vector_result_writer.*` | MySQL row 编码 |

### 19.5 BE 修改

| 文件 | 改造 |
|---|---|
| `internal_service.h/.cpp` | async exec/cancel RPC handler |
| `PInternalService` 实现 | 连接 VectorSearchExecutor |
| `ExecEnv`/`ComputeEnv` | 注入 executor/context manager，或作为 query execution service |
| `CMakeLists.txt` | 新增 vector search 源文件 |
| `service_metrics.*` | RPC/context/task metrics |
| `TabletReader`/`SegmentIterator` | 仅在 V1 复用需要少量入口适配时修改 |

### 19.6 V2 抽取

| 文件 | 说明 |
|---|---|
| `be/src/storage/rowset/segment_vector_searcher.*` | 共享 candidate-first segment search |
| `SegmentIterator` | 调用 `SegmentVectorSearcher` 生成 row ranges/distance |
| `VectorSearchExecutor` | 直接消费 candidates，segment 级调度 |

---

## 20. 第一版实现拆分

### 20.1 Milestone A：语义抽取与规划

1. 抽取 `VectorSearchSpecExtractor`。
2. 使现有 `RewriteToVectorPlanRule` 通过 extractor 生成与之前相同的计划。
3. 新增 `VectorShortCircuitPlanner`。
4. 实现准入/reject reason 和 EXPLAIN。
5. 新增 `ExecPlan` 短路表示。

此阶段必须先保证普通 Pipeline 的向量计划回归不变。

### 20.2 Milestone B：协议和 BE 单节点执行

1. 新增 Protobuf 和 RPC skeleton。
2. 实现 async RPC lifetime/context manager。
3. 实现单 tablet、单 lane 的 VectorSearchExecutor。
4. 复用 TabletReader/SegmentIterator 得到 candidates。
5. 实现 BE TopK 和 MySQL row 编码。
6. 支持 cancel/deadline/profile。

### 20.3 Milestone C：多 tablet 并发和 WorkGroup

1. 实现 bounded lanes + atomic work cursor。
2. 接入目标 WorkGroup ScanExecutor。
3. 实现 lane-local heap 和 final BE merge。
4. 实现队列拒绝、并行度上限和 memory tracker。
5. 增加 tablet boundary yield。

### 20.4 Milestone D：FE 路由和分布式归并

1. 抽取/实现 `VectorSearchRoutingProvider`。
2. 支持 shared-nothing replica 选择。
3. 支持 shared-data warehouse/CN 路由。
4. 同时下发所有节点 RPC。
5. 实现 tablet-level retry/dedup。
6. 实现 FE Global TopK 和 `TResultBatch`。

### 20.5 Milestone E：灰度和完整验证

1. `OFF/AUTO/FORCE` 开关。
2. 完整的 FE/BE profile 和 audit 字段。
3. 普通 Pipeline 与短路 A/B 结果对比。
4. 错误注入、重试、取消、超时和滚动升级测试。
5. 性能基线和高 QPS soak test。

---

## 21. 测试计划

### 21.1 FE 单元测试

- L2 ASC 和 cosine DESC 的 spec 提取。
- distance 函数在 projection/alias/cast 中的匹配。
- vector dimension/index metric 检查。
- scalar/range predicate 拆分。
- 每一个准入条件的 reject reason。
- partition/distribution pruning。
- shared-nothing/shared-data 路由。
- BE 并发下发，验证不是串行 `future.get()`。
- TopK merge、offset 和 tie breaker。
- tablet retry 和去重。
- AUTO/FORCE 回退。

### 21.2 BE 单元测试

- Protobuf 字段和 float32 buffer 校验。
- RPC handler 提交后不同步执行 ANN。
- context lifetime 和 `done->Run()` 只调用一次。
- WorkGroup ScanExecutor 选择。
- bounded lanes，队列 task 数不随 tablet 总数无界增长。
- tablet work cursor 和 tablet boundary yield。
- lane-local heap 和 BE final heap。
- cancel/deadline/first-error-wins。
- queue full/backpressure。
- snapshot/schema/index mismatch。
- HNSW/IVFPQ、filter、delvec、refine、exact fallback。
- MySQL text/binary row 编码。
- query/workgroup memory tracker。

### 21.3 SQL 集成测试

对每个用例比较：

```text
vector_short_circuit_mode = OFF
vector_short_circuit_mode = FORCE
```

覆盖：

- DUP/PRIMARY KEY 表。
- shared-nothing/shared-data。
- 无 filter、高/中/低选择率 filter。
- bitmap/inverted index filter。
- 距离 range + TopK。
- 索引缺失 segment 的 fallback。
- insert/delete/partial update/compaction 并发。
- schema change 并发。
- replica/CN 失效和重试。
- prepared statement binary result row。
- 客户端取消/KILL QUERY。

对于 ANN，在使用完全相同的 index/search params 时，短路和 Pipeline 应生成相同的局部候选和全局排序。对有相同 score 的多行，必须根据明确的 tie-breaker 校验，不使用偶然的执行顺序作为预期结果。

### 21.4 性能测试矩阵

变量：

| 维度 | 测试点 |
|---|---|
| Vector dimension | 128 / 768 / 1536 |
| K | 1 / 10 / 100 / 安全上限 |
| Filter selectivity | 0.01% / 1% / 10% / 100% |
| Tablets per BE | 少 / 中 / 多 |
| Segments per tablet | 1 / 多个小 segment / 少量大 segment |
| Nodes | 1 / 3 / 大规模 |
| Cache | warm / cold |
| Index | HNSW flat / HNSW quantized / IVFPQ |
| Projection | ID only / narrow / wide |
| QPS | single query / low concurrency / saturation / overload |

需要拆分测量：

```text
FE parser/analyzer
FE short-circuit planning
FE routing
FE RPC wait
BE queue wait
BE snapshot/index init
BE ANN
BE materialization
BE reduce/encode
FE global reduce/result
```

检查的性能结果：

- warm index 下 p50/p95/p99 端到端延迟。
- 每个 FE/BE 的 CPU 和内存。
- BE 线程数、runnable threads 和 context switch。
- WorkGroup 之间的延迟隔离。
- 普通 SQL scan 与 vector short circuit 混部时的干扰。
- 超载时是否快速拒绝，而不是无界排队。

### 21.5 BE 执行模型原型验证（2026-08-26）

第一版 BE 执行内核原型已实现：

- bounded ScanTask lanes + atomic work cursor。
- lane-local TopK + final BE TopK。
- WorkGroup ScanExecutor 调度。
- ascending/descending score 和 deterministic tie-breaker。
- first-error-wins、显式 cancel 和 single completion。

独立单元测试在 `starrocks/dev-env-ubuntu:latest` 上通过 6/6，包含 TopK、并行度上限、错误取消、显式取消和参数校验。

微基准环境：

```text
ECS: 32 vCPU / 123 GiB
Docker cpuset: 8 CPUs
Build: Release
Repetitions: 7
TopK: 10
Candidates per work item: 32
```

对比的两种结构都使用相同线程数和同一类 `ScanExecutor`：

1. `VectorSearchBoundedLanes`：有界 lanes，每个 lane 自有 TopK，最后一次归并。
2. `PipelineStyleFanoutSharedTopK`：每个 work item 一个队列任务，所有 producer 写共享 TopK。

Release real-time 均值：

| Workload | Direct bounded lanes | Pipeline-style baseline | Direct 变化 |
|---|---:|---:|---:|
| 8 items / 2 threads / 纯调度 | 17.277 us | 20.789 us | -16.9% |
| 64 items / 4 threads / 纯调度 | 87.473 us | 120.981 us | -27.7% |
| 256 items / 8 threads / 纯调度 | 473.443 us | 539.181 us | -12.2% |
| 64 items / 4 threads / 重计算 | 3.436 ms | 3.499 ms | -1.8% |

初步结论：

- 对细粒度、小索引或小 segment 请求，bounded lanes + lane-local TopK 能明显减少队列任务和共享 heap 争用。
- 当 ANN/距离计算本身占主导时，BE 内部调度结构的收益收敛到低个位数。
- 因此短路的端到端收益不能只依赖 BE 线程模型，还需来自 FE 规划短路、Fragment/Pipeline 初始化消除、BE 级 TopK 减少传输和 late materialization。

限制：该 baseline 是 pipeline-style 调度/归并结构，不包含真实 `FragmentExecutor`、`PipelineDriver`、`Exchange`、FE 规划和 RPC，不能作为完整 SQL 端到端性能结论。下一阶段必须接入真实 TabletReader/vector index，对相同数据、路由、谓词和 ANN 参数执行 short-circuit/Pipeline A/B。

### 21.6 Cohere 1M SQL/Pipeline 端到端基线（2026-08-26）

在香港 ECS `8.218.240.71` 上使用 VectorDBBench `Performance768D1M` 建立现有 SQL/Pipeline 路径基线。StarRocks 版本为 `4c1536df23d914ed9a71aba30d5a74e6b442b4e4`，部署形态为 shared-data，查询阶段 FE/CN 各限制为 16 CPU cores，CN `mem_limit=64G`、Data Cache 32GB。查询参数固定为 K=100、HNSW M=16、efConstruction=300、`ann_params={"efSearch":100}`，客户端使用 MySQL binary prepared statement 和 `CAST(? AS ARRAY<FLOAT>)`。

最终对照表的物理布局为 `1 rowset / 1 standalone segment / 1 HNSW .vi`。构造该布局需要同时满足：

- `max_segment_file_size=8589934592`，避免 Compaction 按文件大小切段。
- `write_buffer_size=8589934592`，避免单次大写入因 MemTable flush 产生多个 segments。
- 表属性 `file_bundling=false`。shared-data 默认 `file_bundling=true` 时，bundle segment 会主动跳过独立 `.vi` 写入，FE 虽可显示 `VECTORINDEX: ON`，BE 仍会 fallback 到 brute-force。
- VectorDBBench pure MySQL socket 设置 `TCP_NODELAY`，消除 prepared execute/fetch 小包触发的约 40ms Nagle/delayed-ACK 客户端延迟。

物理索引命中由 Query Profile 验证：`RawRowsRead=100`、`RowsRead=100`、`SegmentsReadCount=1`、`ScanTime=1.652ms`；错误的 bundle 单段表则为 `RawRowsRead=1,000,000`、`BytesRead=3.58GB`、`ScanTime=833ms`。

| concurrency | QPS | avg latency | p95 | p99 |
|---:|---:|---:|---:|---:|
| 1 | 199.41 | 5.01ms | 5.35ms | 5.56ms |
| 50 | 2605.95 | 19.15ms | 29.04ms | 36.97ms |
| 100 | 2867.84 | 34.73ms | 55.17ms | 67.04ms |
| 200 | 2963.39 | 66.85ms | 109.03ms | 135.11ms |

Serial 1000-query result：recall@100=0.8870，NDCG=0.9038，平均延迟 5.2ms，p95=5.5ms，p99=5.8ms。CN 在 16-core cpuset 下重启后执行（FE `SHOW COMPUTE NODES` 同样报告 16 cores），索引在计时前预热。原始结果位于远端 `/root/vectordb-results/StarRocks/result_20260826_sr-final-cohere1m-ef100-1seg-nodelay-16boot_starrocks.json`，run id 为 `9e56c98e6beb4179b5491e78158e6f7c`。

实验同时暴露两个工程约束：普通 Lake Compaction 事务对 singleton rowset 可能产生 0-input/0-output no-op；并行 Compaction 仅允许单独重写 `overlapped=true` 且 segment 数不少于 2 的 singleton rowset。另一个环境问题是当前 OSS 凭证可以读写数据，但 bucket policy 拒绝 ListObjects，导致 Lake vacuum 返回 403；不影响本次查询结果，但会阻止历史文件回收，正式长期测试前必须补齐 list/delete 权限。

### 21.7 HNSW Flat 与 HNSW SQ8 对照（2026-08-27）

在相同 Cohere 1M、单 standalone segment、M=16、efConstruction=300、efSearch=100、K=100 和 16-core CN 条件下，将 HNSW quantizer 从默认 `flat` 改为 `sq8`，保持 `enable_vector_index_refine=false`。SQ8 Query Profile 为 `RawRowsRead=100`、`SegmentsReadCount=1`、`ScanTime=2.215ms`，确认物理量化索引命中。

| 指标 | HNSW Flat | HNSW SQ8 | SQ8 相对变化 |
|---|---:|---:|---:|
| 索引 cache 占用 | 3.23GB | 0.92GB | -71.4%（约 3.5× 更小） |
| 单段索引重写时间 | 354.7s | 388.4s | +9.5% |
| conc=1 QPS | 199.41 | 183.96 | -7.7% |
| conc=50 QPS | 2605.95 | 2228.07 | -14.5% |
| conc=100 QPS | 2867.84 | 2649.21 | -7.6% |
| conc=200 QPS | 2963.39 | 2786.20 | -6.0% |
| serial avg latency | 5.2ms | 5.7ms | +9.0% |
| serial p99 | 5.8ms | 6.4ms | +10.3% |
| recall@100 | 0.8870 | 0.8874 | 基本持平 |
| NDCG | 0.9038 | 0.9043 | 基本持平 |

结论：当前 HNSW SQ8 的主要价值是显著降低内存和冷加载体积（冷查 1.19s，Flat 为 4.34s），并没有带来查询加速；在所有测试并发下吞吐均低于 Flat，延迟也更高。原始结果位于远端 `/root/vectordb-results/StarRocks/result_20260827_sr-final-cohere1m-ef100-1seg-sq8-nodelay-16boot_starrocks.json`，run id 为 `27181011debe41869649e778b47c4943`。

后续 short-circuit、Pipeline A/B 和线程模型实验统一以 HNSW SQ8、`refine=OFF` 为主基线，HNSW Flat 仅保留作对照。远端正式表 `vdbb_cohere_1m.items` 指向 SQ8 单段表，Flat 表保留为 `items_flat_result`。

### 21.8 SQ8 SQL/Pipeline 阶段耗时拆解（2026-08-27）

在同一 SQ8 单段表上补齐低开销 Profile：FE `Tracers` 保留纳秒值并只在生成 Profile 文本时格式化为 `ns/us/ms`；BE shared-data `LakeDataSource` 只在 `_use_vector_index=true` 时汇总 `SegmentIterator` 已有 ANN 计时，不在 ANN 热循环增加第二套时钟。关闭 Profile 连续预热并执行 1000 条 prepared 查询后，客户端平均延迟为 6.226ms；随后只对目标查询开启 Profile。

| 阶段 | prepared SQL | 文本 SQL | 说明 |
|---|---:|---:|---|
| 客户端耗时（Profile on 单样本） | 7.186ms | 7.861ms | Profile off prepared 1000-query avg=6.226ms |
| Parser | 不重复执行 | 731.724us | prepared execute 只绑定参数 |
| Planner Total | 1.395ms | 1.906ms | 不含文本 SQL 的 Parser |
| Analyzer | 86.696us | 117.958us | Planner 子项 |
| Transformer | 107.944us | 156.851us | Planner 子项 |
| Optimizer | 924.326us | 1.267ms | Planner 最大子项 |
| ├─ RuleBaseOptimize | 508.300us | 734.951us | Optimizer 子项 |
| ├─ CostBaseOptimize | 145.239us | 182.371us | Optimizer 子项 |
| └─ PhysicalRewrite | 170.907us | 226.639us | Optimizer 子项 |
| ExecPlanBuild | 200.759us | 273.931us | PlanFragment 构建 |
| Deploy Total | 1.206ms | 1.474ms | Fragment 下发与确认 |
| ├─ Fragment 序列化 | 227.469us | 348.005us | `DeploySerializeConcurrencyTime` |
| ├─ RPC 发起 | 44.947us | 47.698us | `DeployStageByStageTime` |
| ├─ Async send | 39.238us | 41.770us | 嵌套计时，不与 Deploy 子项重复相加 |
| └─ RPC/BE 初始化确认 | 824.130us | 966.772us | `DeployWaitTime`，Deploy 最大子项 |
| BE critical wall time | 2.740ms | 2.719ms | 三个 fragments 并行执行，取最大 wall time |
| SegmentInit | 971.967us | 719.603us | 包含向量行范围计算 |
| ├─ GetVectorRowRangesTime | 917.610us | 667.434us | 包含下面 ANN 与 ID/距离处理，不能重复相加 |
| ├─ VectorSearchTime | 888.548us | 637.767us | 物理 HNSW SQ8 ANN |
| └─ ID/距离处理 | 23.430us | 24.305us | `ProcessVectorDistanceAndIdTime` |
| SegmentRead | 11.776us | 14.920us | ANN scan 侧 top-100 candidate/row-id 读取；不等同于 Native LOOK_UP 内部存储耗时 |
| └─ BlockFetch | 6.429us | 10.134us | SegmentRead 子项 |
| Fetch RPC | 210.607us | 266.495us | 100 行 late-materialization 请求整体网络往返 |
| Fetch task 生成 | 9.083us | 7.136us | `GenFetchTasksTime` |
| Fetch 输出组装 | 12.320us | 10.076us | `BuildOutputChunkTime` |
| Exchange network | 87.828us | 111.773us | scan fragment 到 merge fragment |

注意：`GetVectorRowRangesTime` 包含 `VectorSearchTime` 和 `ProcessVectorDistanceAndIdTime`，三个值不能相加；多个 fragment 的 `QueryExecutionWallTime` 也相互重叠。Native LOOK_UP 当前 `GetDataFromStorageTime/FillResponseTime` 仍显示 0，现阶段用 Fetch RPC 的 210–266us 作为标量回捞整体上界，不能把这个 0 解释成没有回捞工作。

结论与优化顺序：

1. prepared statement 只省掉约 0.73ms Parser，没有省 Analyzer、CBO、PlanFragment 和 Deploy；prepared Planner 仍为 1.395ms。
2. FE 最大单项是 Optimizer（0.924ms，其中 RuleBase 0.508ms），其次是 Deploy（1.206ms）；Deploy 内 RPC/BE 初始化确认占 0.824ms。
3. ANN 本体约 0.64–0.89ms，低于 `Planner + Deploy` 的约 2.6ms。只优化 HNSW 算法，即使 ANN 再快 20%，端到端也仅减少约 0.13–0.18ms。
4. BE critical wall 为约 2.7ms，而 ANN 低于 0.9ms，剩余主要是 Fragment/Pipeline 调度、等待、Exchange 和 Fetch；这正是 SQL short-circuit BE executor 的主要收益空间。
5. ANN scan 侧 candidate/row-id 读取只有 12–15us；Native LOOK_UP 内部存储计时尚未正确汇总，但整个 Fetch RPC 只有约 0.21–0.27ms，因此它仍明显低于 CBO、Deploy 和 ANN。
6. Profile on 单样本比 Profile off 的 1000-query 平均值高约 0.96ms（另含单次查询噪声），所以正式 QPS/延迟 sweep 必须关闭 Profile，只抽样开启诊断。

原始 Profile 位于远端 `/root/vectordb-results/sq8_prepared_phase_profile.txt` 和 `/root/vectordb-results/sq8_direct_phase_profile.txt`，query ID 分别为 `01a04267-3564-7705-b603-a2ba9d19cdf0` 和 `01a04267-35f9-77a2-8bcc-55d917b6d51e`。

### 21.9 FE prepared vector fast-path A/B（2026-08-27）

第一版保留 MySQL prepared SQL 接口和既有 BE Pipeline，用两个默认关闭的 session 开关分离验证 FE 固定开销：

- `enable_vector_search_plan_cache=true`：第一次执行仍走完整 Analyzer/CBO 并缓存物理计划；后续执行只校验 schema/table 版本、解析并绑定新 query vector，然后重建 PlanFragment。
- `enable_single_node_schedule=true`：对符合条件的 shared-data vector scan 复用现有 single-node batch-fragment 下发，将 3 次 fragment RPC 合并为 1 次。这是 FE-only 的 RPC 减量实验，尚不是最终的紧凑 `PVectorSearchPlan` RPC。

只对“单个 OLAP vector scan、ANN 已开启、`refine=OFF`、无 scan predicate、只有一个 vector literal 参数”的 prepared TopK 查询命中快路；其他查询全部回退原 Planner。每组先预热 100 条，关闭 Profile 测量 1000 个 Cohere1M 不同 query vector，然后单独采集一条 Profile。FE 固定在 CPU 16–31，CN 固定在 CPU 0–15，数据和索引与 21.8 一致（HNSW SQ8，单 segment，`efSearch=100`）。

| 组别 | 第 1 轮 avg | 第 2 轮 avg | 两轮合并 avg | 相对 baseline | 第 2 轮 p50 / p95 / p99 |
|---|---:|---:|---:|---:|---:|
| baseline：原 Analyzer + CBO + 3 RPC | 6.909ms | 5.733ms | 6.321ms | — | 5.702 / 6.286 / 7.069ms |
| plan cache：跳过 Analyzer/CBO | 5.055ms | 4.695ms | 4.875ms | **-22.9%** | 4.686 / 5.143 / 5.651ms |
| plan cache + batch RPC | 4.643ms | 4.526ms | 4.585ms | **-27.5%** | 4.503 / 5.008 / 5.499ms |

两轮前 20 条查询的 TopK ID 序列均与 baseline 完全一致。第二轮 Profile 的关键变化如下：

| 阶段 | baseline | plan cache | plan cache + batch RPC |
|---|---:|---:|---:|
| Planner Total | 1.147ms | 314.721us | 317.666us |
| Analyzer | 74.537us | 已跳过 | 已跳过 |
| Transformer | 112.600us | 已跳过 | 已跳过 |
| Optimizer | 744.365us | 已跳过 | 已跳过 |
| VectorPlanCacheRebind | — | 151.734us | 151.203us |
| ExecPlanBuild | 148.768us | 151.647us | 131.642us |
| Deploy Total | 1.078ms | 1.135ms | 826.492us |
| fragment RPC 数 | 3 | 3 | 1 |
| Deploy wait | 797.523us | 871.677us | 587.517us |
| Deploy data | 28,069B | 28,069B | 29,414B |

结论：FE plan cache 命中后 Planner 从约 1.15ms 降至 0.315ms，跳过 Analyzer/Transformer/CBO 本身带来了两轮合并 **22.9%** 的端到端平均延迟收益。batch-fragment 将 RPC 数 3 降到 1，第二轮 Deploy 减少约 0.31ms，进一步将两轮合并收益扩大到 **27.5%**；但因公共 descriptor 包装，总字节数暂时增加 1,345B。剩余约 4.5ms 中仍包含 PlanFragment 重建、完整 Fragment/Pipeline 初始化与执行、Exchange/Fetch 和 MySQL 结果返回，下一阶段需用紧凑 vector RPC + BE VectorSearchExecutor 消除这些固定开销。

最新一轮原始结果与 Profile 位于 `sr-vdbbench` 容器的 `/results/sq8_fe_fastpath_ab.json` 和 `/results/sq8_fe_fastpath_{baseline,plan_cache,plan_cache_batch}_profile.txt`。

### 21.10 BE direct RPC 与双 executor 线程模型（2026-08-27）

参考 Milvus QueryNode 中“单调度循环负责 pending queue/兼容请求合并，有界执行池负责 SearchTask::Execute”的分工，BE direct vector RPC 采用两个逻辑 executor：

```text
BRPC handler
  -> VectorSearchMergeExecutor (single scheduler thread)
  -> VectorSearchExecutor (WorkGroup ScanExecutor bounded lanes)
  -> Lake TabletReader / SegmentIterator / VectorIndexReader
  -> BE-local TopK
  -> RPC completion
```

V1 的 `VectorSearchMergeExecutor` **不合并请求**：`VectorSearchMergePolicy::try_merge()` 是可注入接口，默认 `NoopVectorSearchMergePolicy` 始终返回 false。它当前只负责有界 pending queue、inflight 准入、query-id 取消、shutdown drain 和 exactly-once completion。不设 micro-batch 等待窗口，串行查询不会因为等待合并增加延迟。后续增加 NQ grouping 时，只替换 merge policy，不改 RPC handler 和实际执行器边界。

`VectorSearchExecutor` 不建独立 vector CPU pool，实际 ANN/tablet work 以 bounded lanes 提交给 WorkGroup ScanExecutor；每个 lane 保持私有 TopK heap，每处理一个 tablet/work item 后 yield，最后一个 lane 执行 BE-local reduce。ScanTask 进入 worker 后安装 WorkGroup mem tracker，避免 direct path 绕过资源组内存记账。

新增内部 RPC：

- `exec_vector_search(PExecVectorSearchRequest) -> PExecVectorSearchResult`
- `cancel_vector_search(PCancelVectorSearchRequest) -> PCancelVectorSearchResult`

BRPC handler 只验证 protobuf、复制 packed float query vector、构造 task 并 enqueue，不同步执行 ANN；`done` 由完成回调在 ScanExecutor 执行结束后触发。请求只携带 query ID、tablet/version、id/vector column、float32 vector、K、result order、efSearch 和并行度，不携带 DescriptorTable、PlanNode、Fragment 或 Pipeline 参数。

存储路径直接复用 Lake `VersionedTablet -> TabletReader -> SegmentIterator -> VectorIndexReader`，因此保留 visible version、delvec、索引 cache 和 fallback 语义，但不构造 ExecNode、FragmentContext、PipelineDriver、Exchange 或 LOOK_UP Fragment。V1 限定 shared-data、HNSW、`refine=OFF`、无 scalar predicate、BIGINT id projection；FE 尚未路由到新 RPC，所以这一阶段不改变已有 SQL 执行路径。

---

## 22. 验收标准

第一版只有同时满足以下条件才可开启 `AUTO`：

1. 支持查询的结果与普通 Pipeline 语义一致。
2. 不支持查询完整回退，不产生错误的半短路计划。
3. BRPC handler 不同步执行索引加载、ANN 或数据读取。
4. 所有 ANN/tablet work 都受 WorkGroup ScanExecutor 管理。
5. 不创建与 CPU cores 叠加的独立 vector CPU 线程池。
6. 每个查询的队列 task 数受 lane 上限约束。
7. FE 同时下发所有节点 RPC，不串行等待。
8. BE 只返回 BE 级 TopK，不返回 per-segment 全量候选。
9. cancel、deadline、replica retry、schema change 和滚动升级具备可预期语义。
10. 超载测试中队列和 context 内存有明确上限。
11. Profile 能区分 FE planning/routing、BE queue、ANN、materialization 和 reduce。
12. 在目标 serving workload 上相比普通 Pipeline 有稳定的 p50/p99 收益，且不明显损害混部普通 SQL。

---

## 23. 风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| 普通 Pipeline 和短路语义分叉 | 结果不一致 | 抽取共享 `VectorSearchSpecExtractor`，复用 TabletReader/SegmentIterator |
| BRPC async lifetime 处理错误 | UAF/double done | context manager + single completion state + 专项并发测试 |
| ANN 占满 ScanExecutor | 普通 SQL scan 延迟上升 | WorkGroup 调度、bounded lanes、tablet yield、混部 benchmark |
| ANN 内外双重并行 | 过度调度/内存带宽饱和 | 默认 ANN 内部单线程，严格控制 outer lanes |
| 单 tablet 含大量 segments | 长任务、yield/cancel 延迟 | V1 限制并行度并监控 tablet time；V2 改 segment 调度 |
| Cold index 加载阻塞 scan worker | 冷查询影响其他 scan | 索引预热/single-flight/metrics；V2 异步 index load executor |
| FE merge 成为高 QPS 热点 | FE CPU 上升 | BE 级 TopK，k-way merge，多 FE 负载均衡；后续分层 reducer |
| 宽 projection 提前读取 | I/O/网络放大 | V1 严格限制 projection 和 K；V2 late materialization/requery |
| shared-data CN 重试 | 重复/版本混合 | 固定 visible version，tablet status，按 tablet ID 去重 |

---

## 24. 备选方案与决策

### 24.1 仅使用 Prepared Statement/Plan Cache

优点是改动小，可减少部分 FE 规划开销。缺点是仍会生成 Fragment/Pipeline，不能消除 BE 固定准备和调度开销。

**决策**：作为后续补充，不代替短路执行。

### 24.2 只在现有 CBO 后执行短路

优点是最小化 FE planner 变更，可以先验证跳过 Pipeline 的收益。缺点是仍支付通用 CBO 代价。

**决策**：可作为 Milestone A/B 期间的内部过渡和 A/B 验证开关，第一版完成形态仍要在 LogicalPlan 后短路。

### 24.3 对外新增 gRPC Search API

优点是可以完全避免 SQL parse/analyze，天然支持 binary vector 和 NQ batch。缺点是需要新服务、新 SDK、认证、负载均衡和运维体系。

**决策**：不在第一版实现；未来复用同一 `PVectorSearchPlan` 和 BE VectorSearchExecutor。

### 24.4 新建独立 vector CPU 线程池

优点是表面上与普通 scan 分离。缺点是容易绕过 WorkGroup、导致 CPU 过度调度和资源组失效。

**决策**：V1 明确不采用。

---

## 25. 后续演进

### V2：Candidate-first 与 segment 级调度

- 抽取 `SegmentVectorSearcher`。
- segment 成为 work item/yield 边界。
- BE 先归并 row locator/score，仅物化 BE TopK。
- 可选 `fetch_vector_search` 和 pinned context。
- 索引 cache miss 异步化。

### V3：Batch 和 Hybrid Search

- SQL prepared/vector plan template cache。
- 跨请求 NQ grouping，按 table/index/filter/deadline 合并。
- dense/sparse/GIN 多路召回。
- RRF/weighted fusion。
- range search 分页/流式返回。

### V4：减少全分片 fan-out

- tablet/segment centroid metadata。
- global coarse vector router。
- semantic partition pruning。
- 分层 reduce coordinator。

这一阶段解决的是数据面扩展问题，与本文的 SQL/Pipeline 固定开销短路互补，不应混为同一个首版任务。

---

## 26. 待通过实验确定的参数

以下参数不在设计阶段直接固化：

1. `vector_search_max_parallelism_per_query` 默认值。
2. ScanExecutor 上 ANN 与普通 scan 的最佳混部比例。
3. HNSW/IVFPQ 是否存在不可关闭的内部多线程。
4. tablet 级 yield 对公平性是否足够，何种 segment 分布必须提前进入 segment task。
5. 宽 projection 达到何种大小后必须使用 late materialization/requery。
6. FE global merge 在何种 BE 数和 QPS 下成为瓶颈。
7. Cold index 加载是否需要在 V1 就独立异步化。

这些参数的最终选择必须同时考虑 p99 latency、吞吐、recall、CPU、内存带宽和普通 SQL 混部干扰。

---

## 27. 最终决策摘要

| 问题 | 决策 |
|---|---|
| 对外是否更换协议 | 否，V1 保留 MySQL + SQL |
| 是否继续通用 CBO | 只对不受支持查询；支持的查询在 LogicalPlan 后短路 |
| 是否生成 PlanFragment | 否 |
| FE -> BE 协议 | 专用 BRPC + Protobuf |
| Query vector 编码 | packed float32 binary |
| BE 执行线程 | WorkGroup OLAP ScanExecutor |
| BRPC handler 是否执行 ANN | 否，必须异步 |
| 是否新建 vector CPU pool | V1 不新建 |
| V1 并行单位 | tablet，bounded lanes |
| ANN 内部线程 | 默认 1 |
| 后续实验默认索引 | HNSW SQ8，refine=OFF；Flat 仅作对照 |
| 归并层次 | Segment -> Tablet -> BE -> FE |
| V1 物化 | 单阶段，BE 局部候选中携带 projection |
| 不支持形态 | 自动回退原 Pipeline |
| 二阶段 fetch/segment scheduler | V2 |
