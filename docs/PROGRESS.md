# 项目进度追踪 — audit-eval

> 最后更新：2026-04-18
> 总体阶段：阶段 0（项目骨架与 Replay Spike）

## 里程碑总览

| 里程碑 | 名称 | Issue 数 | 预估工期 | 状态 | 前置依赖 |
|--------|------|----------|----------|------|----------|
| milestone-0 | 项目骨架与 Replay 格式 Spike | 2 | 2-3 天 | 未开始 | 无 |
| milestone-1 | P2c 在线审计闭环（audit_record / replay_record） | 3 | 3-5 天 | 未开始 | milestone-0 |
| milestone-2 | P2c 基础 retrospective（T+1） | 2 | 3-5 天 | 未开始 | milestone-1 |
| milestone-3 | P5 多时域评估与 drift | 2 | 4-6 天 | 未开始 | milestone-2 |
| milestone-4 | P10 回测能力 | 2 | 4-7 天 | 未开始 | milestone-3 |

## Issue 明细

### milestone-0 — 项目骨架与 Replay 格式 Spike

| ID | 标题 | 优先级 | 状态 | 依赖 |
|----|------|--------|------|------|
| ISSUE-001 | 项目骨架与内部包边界初始化 | P0 | 未开始 | 无 |
| ISSUE-002 | Replay 格式与 manifest 对账 Spike | P0 | 未开始 | #ISSUE-001 |

**退出条件（§21 阶段 0）**：给定 sample cycle，能不调用模型完成 replay 重建；`make ci` 全绿。

### milestone-1 — P2c 在线审计闭环

| ID | 标题 | 优先级 | 状态 | 依赖 |
|----|------|--------|------|------|
| ISSUE-003 | AuditRecord / ReplayRecord 正式 schema 与 AuditWriteBundle 契约 | P0 | 未开始 | #ISSUE-002 |
| ISSUE-004 | persist_audit_records / persist_replay_records 持久化实现 | P0 | 未开始 | #ISSUE-003 |
| ISSUE-005 | replay_cycle_object 查询接口与 ReplayView 重建 | P0 | 未开始 | #ISSUE-004 |

**退出条件（§21 阶段 1）**：一轮 formal cycle 能稳定留下完整审计与回放记录。

### milestone-2 — P2c 基础 retrospective

| ID | 标题 | 优先级 | 状态 | 依赖 |
|----|------|--------|------|------|
| ISSUE-006 | RetrospectiveEvaluation schema 与 T+1 compute_retrospective | P0 | 未开始 | #ISSUE-005 |
| ISSUE-007 | Retrospective summary 与累积告警 | P1 | 未开始 | #ISSUE-006 |

**退出条件（§21 阶段 2）**：最近若干 cycle 能看见可解释的偏差曲线和摘要。

### milestone-3 — P5 多时域评估与 drift

| ID | 标题 | 优先级 | 状态 | 依赖 |
|----|------|--------|------|------|
| ISSUE-008 | T+5 / T+20 回填与多 horizon 支持 | P1 | 未开始 | #ISSUE-007 |
| ISSUE-009 | Evidently drift_report 与第三层结构性告警 | P1 | 未开始 | #ISSUE-008 |

**退出条件（§21 阶段 3）**：长期评估闭环形成，drift 规则可稳定输出 analytical 资产。

### milestone-4 — P10 回测能力

| ID | 标题 | 优先级 | 状态 | 依赖 |
|----|------|--------|------|------|
| ISSUE-010 | Point-in-time 检查器与 BacktestResult schema | P2 | 未开始 | #ISSUE-009 |
| ISSUE-011 | Alphalens 回测集成与 backtest_result 写入 | P2 | 未开始 | #ISSUE-010 |

**退出条件（§21 阶段 4）**：至少一种历史信号可产出可解释的信号质量报告；PIT 检查硬前置。

## 关键守门指标（§19.2）

| 指标 | 目标值 |
|------|--------|
| formal LLM 调用 replay 字段缺失率 | 0 |
| replay 错读非 manifest 版本发生率 | 0 |
| retrospective 多时域回填完成率 | 100% |
| drift 告警误写在线控制字段次数 | 0 |
| backtest PIT 检查通过前发布次数 | 0 |

## 边界红线（CLAUDE.md 同步）

- 任何 issue 不得写 `feature_weight_multiplier`。
- replay 唯一合法模式为 `read_history`，不得重调模型。
- 所有 replay / retrospective 必须先经过 `cycle_publish_manifest`。
- backtest 必须通过 PIT 检查才能入库。
- P10 之前不引入 NautilusTrader 或第二套研究平台。
