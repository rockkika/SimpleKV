# SimpleKV

这是我在学习分布式系统时完成的一组课程作业，原始要求可以从[作业页面](https://pdos.csail.mit.edu/6.824/)查看。项目从 MapReduce 和一个带版本号的 KV Server 开始，逐步实现 Raft、基于 Raft 的 KV 服务，最后再把数据拆到多个 shard group 中，并处理配置变更、数据迁移和 controller 恢复。代码主要用于学习和实验，不是生产级实现。

一开始我没有完全理解 Raft 就开始做，认为有 AI 手把手教我写代码，我会慢慢理解。但实际上这是一个大坑：刚开始做时，我会让 AI 一行一行解释代码、解释为什么这么做，结果却是越写越懵。我认为开始这份作业前必须真正理解 [Raft](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)，包括它为什么比 Multi-Paxos 更“understandable”。最好也先读懂 [single-decree Paxos](https://pdos.csail.mit.edu/6.824/papers/paxos-simple.pdf)以及它如何扩展成 Multi-Paxos：single-decree Paxos 本身很容易理解，而顺着论文感受 Paxos 如何一步步推导，再看看扩展到 Multi-Paxos 时困难出在哪里，也就会慢慢理解 Raft 为什么要做出那些设计。

我认为稳定理解 Raft 最重要的抓手不是心跳，而是“多数派一定相交”。一条日志被提交需要多数派确认，一个新 leader 当选也需要多数派投票，所以提交多数派和之后的选举多数派之间一定至少有一个共同节点。仅有相交还不够，Raft 又通过投票时的日志新旧检查、term 和日志匹配规则，让已经提交的历史不能被一个日志更旧的 candidate 绕过去。于是更准确的说法是：**下一个 leader 不一定来自上一次成功复制日志的那批节点，但它必须获得一个与旧多数派相交的选举多数派，并且自己的日志足够新，因此不会丢掉已经提交的日志。** [Raft 论文](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)把这些约束组织成了比较容易实现的 leader、term 和连续日志；[Paxos 论文](https://pdos.csail.mit.edu/6.824/papers/paxos-simple.pdf)背后也有同一个核心想法：不同 quorum 必然相交，后来的提案必须继承交点中已经接受的值。

实现过程中踩过不少坑，印象最深的是这些：

- 一开始每次心跳或 `Start()` 都可能为同一个 follower 新建一个发送 `AppendEntries` 的 goroutine。不同 RPC 是并发的，后发请求可能先收到回复；如果新的成功回复已经把 `nextIndex` 推进了，旧的失败回复才回来，又按旧状态把它回退，就会出现进度倒退。后来改成每个 follower 只有一个长期 replication worker，`Start()` 和心跳只负责唤醒它，同一个 follower 的复制状态由一条执行流维护。
- shard 外层 Clerk 知道当前配置，内层 shard group Clerk 只知道某一组固定的服务器。内层 `Get`/`Put` 如果对旧 group 死循环，shard 已经迁走时外层就永远拿不回控制权，也就没机会重新查询配置。这里内层只尝试一轮服务器，然后把失败交还给外层，由外层刷新配置再决定请求发到哪里。
- RPC 没收到回复不代表服务端没执行。`Put` 重试后遇到 `ErrVersion` 时，客户端无法区分“第一次已经成功但回复丢了”和“第一次就被其他写入抢先了”，所以需要把这种不确定性作为 `ErrMaybe` 交给上层处理。
- 做 snapshot 后，日志下标不再等于 Go slice 下标。需要一直区分逻辑日志 index 和内存中的相对位置，并把裁剪后的 Raft 状态与对应 snapshot 一起持久化，否则重启、快速回退和 `InstallSnapshot` 很容易在边界上出错。
- 无限重试虽然有时符合接口语义，但不能写成没有停顿的忙循环。全量测试时，旧请求会一直抢占 CPU 和序列化锁；在失败重试之间加一个很短的退避，系统才能给真正可以推进的请求留出空间。
