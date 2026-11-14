# 数据血缘影响分析 API（FastAPI + SQLite）

本地部署的血缘影响分析服务：批量登记**源字段 / 派生字段 / 报表列**的依赖关系，
提交拟下线字段后，递归计算**确实失效的报表列**，并给出每个失效列各候选输入组
缺失的具体依赖。

## 语义规则

- 每个下游列可登记**多个候选输入组**（`candidate_groups`，组的数组）。
- 一个候选组可用 ⇔ 组内字段**全部**可用。
- 一个列可产出 ⇔ **任一**候选组可用；仍有可用组的列不会被误报。
- 源字段（`source`）没有输入；派生 / 报表列必须至少有一个非空候选组。
- 提交拟下线字段后沿血缘图递归传播：
  - 只有所有候选组都不可用的列才计入失效；
  - 返回失效报表列的**每一个**失效候选组及组内缺失字段；
  - 每个缺失字段附带 `root_causes`（追溯到最深层的拟下线字段，而非中间节点）。
- 登记批次若引入循环依赖（含自环），**整批回滚不生效**。

## 目录结构

```
lineage-api/
├── app/
│   ├── main.py         # FastAPI 入口、异常映射、生命周期
│   ├── service.py      # 整批事务登记、循环检测、递归失效分析、同步快照/分页
│   ├── db.py           # SQLite 连接 / WAL / 快照与写事务 / 事件表 schema
│   ├── cursor.py       # 同步分页游标的不透明编解码
│   ├── models.py       # Pydantic 请求与响应模型
│   └── exceptions.py   # 领域错误（422/409/404/400）
├── tests/              # pytest：功能用例 + 真实 uvicorn 并发用例
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── examples/           # 可直接调用的请求样例
```

## Docker Compose 启动

```bash
cd lineage-api
docker compose up -d --build
docker compose ps          # health: healthy
curl http://localhost:8000/health
```

- 服务监听 `8000`，SQLite 数据库落在命名卷 `lineage-data` 的
  `/data/lineage.db`（WAL 模式），容器重建后数据保留。
- 交互式文档：http://localhost:8000/docs （OpenAPI 3.1）。
- 停止与清理：

```bash
docker compose down          # 停止，保留数据卷
docker compose down -v       # 同时删除数据卷
```

## 本地（无 Docker）运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
LINEAGE_DB_PATH=./lineage.db uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API 入口

### 1. 批量登记血缘 `POST /lineage/register`

同一批次内的节点可以互相引用；重复登记同名节点会**整体替换**其候选组。
任何校验失败、类型冲突或循环依赖都返回错误且整批不落库。

```bash
curl -s -X POST http://localhost:8000/lineage/register \
  -H 'Content-Type: application/json' \
  -d @examples/register.json
```

请求体：

```json
{
  "nodes": [
    {"name": "order_amt", "kind": "source"},
    {"name": "coupon_amt", "kind": "source"},
    {"name": "net_amt", "kind": "derived",
     "candidate_groups": [["order_amt", "coupon_amt"]]},
    {"name": "report_gmv", "kind": "report",
     "candidate_groups": [["net_amt"], ["order_amt"]]}
  ]
}
```

错误约定：

| HTTP | error                     | 场景                                       |
|------|---------------------------|--------------------------------------------|
| 422  | `validation_error`        | 空候选组、源字段带输入、引用未登记字段等   |
| 409  | `cycle_detected`          | 批次将引入循环依赖，整批回滚（附 cycle 路径）|
| 409  | `conflict`                | 同名节点已存在但类型不同                   |

### 2. 下线影响分析 `POST /lineage/impact`

```bash
curl -s -X POST http://localhost:8000/lineage/impact \
  -H 'Content-Type: application/json' \
  -d '{"retiring": ["coupon_amt"]}'
```

响应（节选）：

```json
{
  "retiring": ["coupon_amt"],
  "failed_report_columns": [
    {
      "name": "report_coupon_rate",
      "failed_groups": [
        {
          "group_index": 0,
          "inputs": ["coupon_amt", "order_amt"],
          "missing": [
            {"name": "coupon_amt", "kind": "source", "root_causes": ["coupon_amt"]}
          ]
        }
      ]
    }
  ],
  "unavailable_count": 3,
  "unavailable_nodes": ["coupon_amt", "net_amt", "report_coupon_rate"]
}
```

- `failed_report_columns` 只包含**所有候选组均失效**的报表列；
  仍有可用组（如上例中的 `report_gmv`，备用组 `[order_amt]` 可用）不会出现。
- `unavailable_nodes` 列出全部不可用节点（含中间派生列），方便逐级排查。
- 拟下线字段未登记时返回 `404 not_found`。

### 3. 查看当前血缘 `GET /lineage/graph`

返回最近一次已提交的完整血缘（节点类型 + 全部候选组）。

### 4. 向另一套元数据目录同步 `GET /lineage/sync`

用于停机 / 恢复期把字段血缘同步到另一套元数据目录。同一字段在同一时期
可能被多次覆盖登记，该接口提供「**全量快照 + 有序增量批次**」两段式同步：

- **首次请求**（不带 `cursor`，或显式传 `cursor=start`）：返回当前完整血缘
  `snapshot` 与首个不透明游标 `next_cursor`，此时 `batches` 为空。
- **后续请求**：把上一次响应的 `next_cursor` 原样回传，按登记顺序返回该游标
  之后的**有序登记批次**与新游标，直到 `has_more=false`。
- 每个批次保持完整、绝不跨页拆分；批次内携带该批**提交时**各字段的定义副本，
  后续覆盖只改当前血缘，不会回改历史事件。
- 游标对客户端不透明，只需原样传递；服务端按单调递增的批次序号 `seq` 取
  `seq > 游标` 的数据，连续读取不漏批次、不重复批次。

```bash
# 首次：拿到完整快照 + 游标
curl -s http://localhost:8000/lineage/sync

# 增量：回传游标，按页拉取登记批次（limit 可选，默认 100，范围 1..1000）
curl -s "http://localhost:8000/lineage/sync?cursor=<next_cursor>&limit=100"
```

首次响应（节选）：

```json
{
  "snapshot": {
    "nodes": [
      {"name": "order_amt", "kind": "source", "candidate_groups": []},
      {"name": "net_amt", "kind": "derived",
       "candidate_groups": [["order_amt", "coupon_amt"]]}
    ],
    "counts": {"source": 2, "derived": 1, "report": 0}
  },
  "batches": [],
  "next_cursor": "bGluMToy",
  "has_more": false
}
```

增量响应（节选）：

```json
{
  "snapshot": null,
  "batches": [
    {
      "seq": 3,
      "created_at": "2026-11-14T10:00:00+00:00",
      "node_count": 2,
      "nodes": [
        {"name": "s3", "kind": "source", "candidate_groups": []},
        {"name": "r1", "kind": "report",
         "candidate_groups": [["s3"]]}
      ]
    }
  ],
  "next_cursor": "bGluMToz",
  "has_more": false
}
```

错误约定：游标损坏 / 无法识别返回 `400 invalid_cursor`；
`limit` 越界返回 `422`。

同步语义与一致性保证：

1. **登记成功即血缘与事件同时可见**：事件行与血缘更新写在同一个
   `BEGIN IMMEDIATE` 事务里一起提交；校验失败 / 循环依赖的批次整批回滚，
   不产生任何事件。
2. **历史定义不可变**：事件表独立保存每批提交时的字段定义副本，同一字段被
   反复覆盖时，旧事件里的定义保持不变，不会被当前定义覆盖。
3. **不漏不重且有序**：批次序号由 `AUTOINCREMENT` 在串行写事务内单调分配，
   分页只取严格大于游标的 `seq` 并按升序返回；每页查 `limit+1` 行判定边界，
   因此批次不会被拆分。
4. **快照与游标对齐**：首次请求的血缘与水位线游标在同一个 WAL 快照事务内
   读取，游标之后的分页绝不会重复返回已包含在快照里的批次。

### 5. 健康检查 `GET /health`

## 并发与一致性设计

1. **WAL 模式**：读写不互相阻塞。
2. **登记串行化**：每个登记批次使用 `BEGIN IMMEDIATE` 先拿写锁，
   并发批次排队执行，配合 `busy_timeout` 避免 `database is locked`。
3. **查询快照隔离**：每次影响分析 / 血缘查询在单连接显式事务内完成，
   SQLite 的快照读保证请求只看到**登记前或登记后**的完整血缘，
   不可能读到应用了一半的批次。
4. **整批原子性**：节点与候选组分两阶段写入（先全部节点、再全部边），
   写入后做全图 DFS 环检测，发现环则抛错触发 `ROLLBACK`，批次内任何数据都不留存。

## 测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

覆盖：候选组 AND / OR 语义、递归失效与根因追溯、备用组存活不误报、
循环 / 自环整批回滚、各类 422/409/404、重复登记替换、
以及在真实 uvicorn 服务上持续 4 秒的并发读写（读者只观测到完整快照）。

同步接口另含 `tests/test_sync.py`（快照与游标对齐、分页不重不漏不拆批、
覆盖字段的历史定义保留、失败批次无事件、非法游标 / limit 校验）与
`tests/test_sync_concurrency.py`（真实 uvicorn 下登记与同步读取并发，
验证连续游标遍历不漏批 / 不重复、批次完整、历史定义不被当前定义覆盖、
失败批次不产生事件）。
