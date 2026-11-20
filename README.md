# 河湾双站水质比测

河湾水务质控组从上游采样簿接收具名原包，同时在下游测站记录本站实测。两站独立关班后，质控员可先定位缺件，补齐后签发固定班报。下游站库持有班报、签报回执、逐笔原码和出处，可供停源后的夜班领取。

## 运行

需要 Python 3.11，仅使用标准库。在项目根目录执行回归和演示。

```sh
python3 -B check_project.py
python3 -B demo.py
```

控制台物化时提供 `check_project.py`。原 unittest 套件也可直接运行。

```sh
python3 -B -m unittest discover -s tests -t . -v
```

演示在临时目录使用两个 SQLite 站库。上游录入原码 3 和 5，按 `2x+1` 得到 7 和 11 μL。投递端先送 U2 并重投，下游用自己的 `3x+2` 口径把本站原码 1 换算为 5 μL。两站关班后先报缺 U1。补收后签出 18 和 5 μL，关闭源端并重开下游可领取同一成果。

## Python 入口

| 包 | 操作 | 结果 |
| --- | --- | --- |
| `shift` | `open_station_store(path, role)` | 打开或初始化角色为 `upstream` 或 `downstream` 的站库，连接由调用方关闭 |
| `shift` | `open_shift(db, pair, role, upstream_points, downstream_points, upstream_calibration, downstream_calibration)` | 冻结配对、两份测点清册和两边定标 |
| `source` | `record_upstream(db, upstream_shift, point, raw, origin)` | 登记带源序、出处和精确微升值的原读 |
| `source` | `dispatch(db, upstream_shift, read.read_no)` | 持久保存首发原包，重复请求返回原包 |
| `source` | `deliver_packet(packet, receive)` / `deliver_in_order(packets, receive)` | 按调用方给定次序调用接纳入口，返回处理结果 |
| `receiver` | `admit(db, downstream_shift, packet)` | 返回固定首纳回执 |
| `local` | `record_local(db, downstream_shift, point, raw)` | 按本站序登记原码和微升值 |
| `source` / `local` | `close_upstream(db, upstream_shift)` / `close_local(db, downstream_shift)` | 关闭相应采样，返回固定清单 |
| `report` | `issue_normal(db, downstream_shift, upstream_manifest)` | 返回 `MissingPackets` 或含 `ReportReceipt` 的 `NormalReport` |
| `report` | `read_normal(db, report_no)` | 按号读取固定正文和原回执 |

`ShiftPair` 的字段顺序为上游站、上游班次、下游站、下游班次。定标用 `Calibration(scale, offset)`。`SourceOrigin` 包含采集站、班次、采样簿及簿内条目。`demo.py` 展示组装这些输入、使用投递端及停源后领取的完整调用。

同一站库固定角色。本站班次号在库内唯一，同一对端站班身份只能绑定一个本站班次。两站使用同样的配对和配置，各自从本站快照取得换算口径。清册按成员比较，重复测点无效。重复初始化与同参重开保全既有业务记录。两站各用整数公式 `raw * scale + offset`，原码和参数应是 Python `int`。零值作为原读保留，有效空班报的合计为零。

上游源序按原读登记生成，实际发件以首发账为准。首纳序和本站采样序独立。重投相同原件复用首纳回执，重投异内容或送错班次拒绝。上游尚有待发读数时关班失败，采集仍开放。两侧重复关班返回原清单，下游关闭采样后可以继续接收迟到原件。签报输入来自可信站对的上游关班操作，下游核对清单配对、序列和实收。完整签报按上游源序和本站采样序逐笔列出，合计从正文原读计算。相同关班输入再次签发返回原班报。

未知班次、原读或报告号返回 `KeyError`，非法字段、错误配对、冻结配置冲突或同号异内容返回 `ValueError` 或其子类。已关闭采样、本站未关或上游有待发原读返回 `RuntimeError` 或其子类。合法缺件通过 `MissingPackets.missing` 按源序返回。`issue_normal` 可接收仅在完整新班报待提交时调用的 `before_commit(report_no)` 钩子，钩子抛错将回滚正文与回执。SQLite 操作错误保留原异常。
