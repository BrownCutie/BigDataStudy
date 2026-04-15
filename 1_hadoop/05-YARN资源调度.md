# 05 - YARN 资源调度

## 一、为什么需要资源调度？

### 1.1 MapReduce 1.0 的问题

在 Hadoop 1.x 时代，MapReduce 既是**计算框架**又是**资源管理器**。JobTracker 同时负责两件事：管理集群资源和调度 MapReduce 任务。

```
┌──────────────────────────────────────────────────────────────┐
│            MapReduce 1.0 架构（Hadoop 1.x）                   │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│           ┌──────────────────────────────┐                   │
│           │       JobTracker             │                   │
│           │  ┌─────────┐ ┌────────────┐ │                   │
│           │  │资源管理  │ │ 任务调度   │ │                   │
│           │  │(内存/CPU)│ │(Map/Reduce)│ │                   │
│           │  └─────────┘ └────────────┘ │                   │
│           └──────────┬───────────────────┘                   │
│                      │                                       │
│        ┌─────────────┼─────────────┐                        │
│        ▼             ▼             ▼                        │
│   ┌─────────┐  ┌─────────┐  ┌─────────┐                    │
│   │TaskTracker│  │TaskTracker│  │TaskTracker│                │
│   │ (slot)   │  │ (slot)   │  │ (slot)   │                  │
│   └─────────┘  └─────────┘  └─────────┘                    │
│                                                              │
│  问题:                                                       │
│  1. JobTracker 是单点，内存压力大（管理所有任务）            │
│  2. 资源和计算耦合，只能运行 MapReduce 任务                   │
│  3. TaskTracker 的 slot 数量固定，无法动态调整                │
│  4. 最大支持约 4000 个节点                                   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 1.2 YARN 的诞生

YARN（Yet Another Resource Negotiator，"又一个资源协商器"）将**资源管理**和**任务调度**分离开来，使 Hadoop 从"只能跑 MapReduce"进化为"能跑各种计算框架"的通用平台。

```
┌──────────────────────────────────────────────────────────────┐
│              YARN 的设计思想                                   │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  MapReduce 1.0:  JobTracker = 资源管理 + 任务调度            │
│                                                              │
│  YARN:         ResourceManager = 资源管理（通用）            │
│                ApplicationMaster  = 任务调度（各框架自定）   │
│                                                              │
│  好处:                                                       │
│  ┌────────────────────────────────────────────────────┐     │
│  │  1. 通用资源管理: 不限于 MapReduce                   │     │
│  │     MapReduce、Spark、Flink、Tez 都可以跑在 YARN 上  │     │
│  │                                                     │     │
│  │  2. 可扩展性: 支持上万节点                          │     │
│  │                                                     │     │
│  │  3. 多租户: 不同用户/团队共享集群资源                │     │
│  │                                                     │     │
│  │  4. 多版本: 同一集群可运行不同版本的计算框架          │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 二、YARN 架构详解

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                         YARN 架构图                                  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                   Client（客户端）                            │  │
│  │        提交作业、查看作业状态、杀死作业                       │  │
│  └────────────────────────┬─────────────────────────────────────┘  │
│                           │                                         │
│                           │ 提交作业                                │
│                           │                                         │
│  ┌────────────────────────▼─────────────────────────────────────┐  │
│  │                                                               │  │
│  │    ┌────────────────────────────────────────────────────┐     │  │
│  │    │              ResourceManager (RM)                   │     │  │
│  │    │                                                   │     │  │
│  │    │  ┌──────────────────┐  ┌────────────────────────┐ │     │  │
│  │    │  │                  │  │                        │ │     │  │
│  │    │  │  Scheduler       │  │  ApplicationsManager   │ │     │  │
│  │    │  │  (调度器)        │  │  (应用管理器)          │ │     │  │
│  │    │  │                  │  │                        │ │     │  │
│  │    │  │  - 分配资源      │  │  - 接收作业提交        │ │     │  │
│  │    │  │  - 不参与监控    │  │  - 启动 ApplicationMaster│ │     │  │
│  │    │  │  - 不负责重启    │  │  - 监控 AM 状态         │ │     │  │
│  │    │  │                  │  │  - AM 失败后重启       │ │     │  │
│  │    │  └──────────────────┘  └────────────────────────┘ │     │  │
│  │    │                                                   │     │  │
│  │    └────────────────────────────────────────────────────┘     │  │
│  │                                                               │  │
│  └────────────────────────────┬──────────────────────────────────┘  │
│                               │                                     │
│                    资源分配 + 容器启动                                │
│                               │                                     │
│  ┌────────────────────────────┼──────────────────────────────────┐  │
│  │                            │                                  │  │
│  │  ┌─────────────────────────▼──────────────────────────────┐  │  │
│  │  │              NodeManager (NM) - 每台机器一个             │  │  │
│  │  │                                                        │  │  │
│  │  │  ┌──────────────────────────────────────────────────┐ │  │  │
│  │  │  │                   Container                      │ │  │  │
│  │  │  │  ┌────────────────┐  ┌─────────────────────────┐│ │  │  │
│  │  │  │  │                │  │                         ││ │  │  │
│  │  │  │  │ApplicationMaster│  │  任务容器              ││ │  │  │
│  │  │  │  │(AM)            │  │  (Map Task / Reduce Task)│ │  │  │
│  │  │  │  │                │  │                         ││ │  │  │
│  │  │  │  └────────────────┘  └─────────────────────────┘│ │  │  │
│  │  │  │                                                │ │  │  │
│  │  │  │  Container = CPU + 内存 的逻辑隔离单位           │ │  │  │
│  │  │  └──────────────────────────────────────────────────┘ │  │  │
│  │  │                                                        │  │  │
│  │  │  职责:                                                  │  │  │
│  │  │  - 管理本机的 Container                                 │  │  │
│  │  │  - 向 RM 汇报资源使用情况                               │  │  │
│  │  │  - 处理来自 AM 的 Container 启动/停止请求               │  │  │
│  │  │                                                        │  │  │
│  │  └────────────────────────────────────────────────────────┘  │  │
│  │                                                              │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心组件说明

| 组件 | 职责 | 类比 |
|------|------|------|
| **ResourceManager (RM)** | 全局资源管理和调度 | 公司总经理 |
| **Scheduler** | 根据策略分配资源给应用 | 人事经理 |
| **ApplicationsManager** | 管理应用的提交和 AM 的生命周期 | 项目经理 |
| **NodeManager (NM)** | 单机资源管理和 Container 管理 | 部门主管 |
| **ApplicationMaster (AM)** | 单个应用的任务调度和监控 | 项目负责人 |
| **Container** | CPU + 内存的逻辑封装 | 办公工位 |

### 2.3 三种角色对比

```
┌──────────────────────────────────────────────────────────────┐
│              YARN 中的三种角色                                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ResourceManager (全局唯一)                                  │
│  ├── 知道集群有多少资源（总CPU、总内存）                     │
│  ├── 知道每个 NM 有多少可用资源                              │
│  ├── 不关心具体任务怎么运行                                   │
│  └── 负责公平地分配资源                                      │
│                                                              │
│  ApplicationMaster (每个作业一个)                            │
│  ├── 知道这个作业需要多少资源                                │
│  ├── 知道任务之间的依赖关系（Map 先，Reduce 后）             │
│  ├── 向 RM 申请资源                                         │
│  ├── 与 NM 通信，启动具体任务                                │
│  └── 监控任务执行，处理失败重试                              │
│                                                              │
│  NodeManager (每台机器一个)                                  │
│  ├── 管理本机的 CPU 和内存                                   │
│  ├── 启动和销毁 Container                                    │
│  ├── 监控 Container 的资源使用                               │
│  └── 向 RM 定期汇报心跳                                      │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 三、yarn-site.xml 配置详解

编辑 `$HADOOP_HOME/etc/hadoop/yarn-site.xml`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

    <!--
        yarn.nodemanager.aux-services
        ─────────────────────────────
        NodeManager 需要启用的辅助服务。
        MapReduce 必须配置为 mapreduce_shuffle，
        因为 MapReduce 的 Shuffle 阶段需要这个服务来传输数据。

        如果你不配置这个，MapReduce 作业提交后会失败，
        错误信息中会提到找不到 shuffle 服务。
    -->
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>

    <!--
        yarn.resourcemanager.hostname
        ────────────────────────────
        ResourceManager 所在的主机名。
        伪分布式模式下设为 localhost。
    -->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>localhost</value>
    </property>

    <!--
        yarn.resourcemanager.webapp.address
        ───────────────────────────────────
        ResourceManager Web UI 的地址和端口。
        浏览器访问这个地址可以查看集群资源和作业状态。
    -->
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>0.0.0.0:8088</value>
    </property>

    <!--
        yarn.nodemanager.resource.memory-mb
        ───────────────────────────────────
        NodeManager 可用的物理内存总量（MB）。

        默认值: 8192 (8GB)

        这个值应该设置为机器物理内存减去操作系统和其他
        服务使用的内存。例如 16GB 的机器，可以设为 12288 (12GB)。

        注意: 如果机器内存较小（如 8GB），建议设为 4096 或更低。
    -->
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>4096</value>
    </property>

    <!--
        yarn.nodemanager.resource.cpu-vcores
        ──────────────────────────────────
        NodeManager 可用的 CPU 虚拟核心数。

        默认值: 8

        建议设置为物理 CPU 核心数。
        在 macOS 上可以通过 sysctl -n hw.ncpu 查看。
    -->
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>4</value>
    </property>

    <!--
        yarn.scheduler.minimum-allocation-mb
        ───────────────────────────────────
        每个容器申请的最小内存（MB）。

        默认值: 1024 (1GB)

        任何 Container 请求的内存必须 >= 这个值。
        如果你的任务内存较小，可以降低到 512。
    -->
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>1024</value>
    </property>

    <!--
        yarn.scheduler.maximum-allocation-mb
        ───────────────────────────────────
        每个容器申请的最大内存（MB）。

        默认值: 8192 (8GB)

        不能超过 yarn.nodemanager.resource.memory-mb 的值。
    -->
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>4096</value>
    </property>

    <!--
        yarn.nodemanager.vmem-check-enabled
        ───────────────────────────────────
        是否检查 Container 的虚拟内存使用量。

        默认值: true

        如果 Container 使用的虚拟内存超过
        (物理内存 × yarn.nodemanager.vmem-pmem-ratio)，
        Container 会被杀死。

        学习阶段建议设为 false，避免因虚拟内存检查导致
        任务被误杀。
    -->
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>

    <!--
        yarn.nodemanager.pmem-check-enabled
        ───────────────────────────────────
        是否检查 Container 的物理内存使用量。

        默认值: true

        如果 Container 使用的物理内存超过申请的值，
        Container 会被杀死。

        学习阶段可以设为 false，但生产环境建议 true。
    -->
    <property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
    </property>

    <!--
        yarn.log-aggregation-enable
        ─────────────────────────
        是否启用日志聚合。

        启用后，任务运行在各 NodeManager 上的日志会被
        收集到 HDFS 上的统一目录，方便查看。

        默认值: false
        建议值: true
    -->
    <property>
        <name>yarn.log-aggregation-enable</name>
        <value>true</value>
    </property>

</configuration>
```

## 四、启动 YARN

### 4.1 启动命令

```bash
# 确保 HDFS 已经启动
start-dfs.sh

# 启动 YARN
start-yarn.sh

# 预期输出:
# Starting resourcemanager
# Starting nodemanagers
```

### 4.2 验证启动

```bash
# 方法 1: jps 命令
jps
# 预期看到以下额外进程:
# 12345 ResourceManager
# 12456 NodeManager
# （加上之前的 NameNode、DataNode、SecondaryNameNode）

# 方法 2: yarn 命令
yarn node -list
# 输出:
# Total Nodes:1
#          Node-Id             State    Node-State-Http-Address   Number-of-Running-Containers
#  localhost:12345             RUNNING  localhost:8042            0
```

### 4.3 YARN 启动后的完整进程

```
┌──────────────────────────────────────────────────────────────┐
│              YARN 启动后的完整 Java 进程                       │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  jps 输出:                                                   │
│                                                              │
│  HDFS 进程:                                                  │
│  ├── NameNode           (端口: 8020 RPC, 9870 Web)          │
│  ├── DataNode           (端口: 9866 数据, 9864 Web)         │
│  └── SecondaryNameNode  (端口: 9868 Web)                    │
│                                                              │
│  YARN 进程:                                                  │
│  ├── ResourceManager    (端口: 8032 RPC, 8088 Web)         │
│  └── NodeManager        (端口: 8042 Web)                    │
│                                                              │
│  端口汇总:                                                   │
│  ┌────────────────────────────────────────────────────┐     │
│  │  端口   │ 服务              │ 协议                   │     │
│  │  ─────────────────────────────────────────────     │     │
│  │  8020   │ NameNode RPC     │ Hadoop IPC            │     │
│  │  9870   │ NameNode Web     │ HTTP                  │     │
│  │  9868   │ SNN Web          │ HTTP                  │     │
│  │  8032   │ RM RPC           │ Hadoop IPC            │     │
│  │  8088   │ RM Web           │ HTTP                  │     │
│  │  8042   │ NM Web           │ HTTP                  │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 五、提交 MapReduce 作业到 YARN 的完整流程

### 5.1 作业提交流程

```
┌─────────────────────────────────────────────────────────────────────┐
│              MapReduce 作业提交到 YARN 的完整流程                      │
│                                                                     │
│  Client          RM              NM            AM           NM      │
│    │              │               │             │            │      │
│    │ ① 提交作业   │               │             │            │      │
│    │─────────────→│               │             │            │      │
│    │              │               │             │            │      │
│    │              │ ② ApplicationsManager         │            │      │
│    │              │    接收作业，分配 AppID         │            │      │
│    │              │               │             │            │      │
│    │              │ ③ 在某个 NM 上   │             │            │      │
│    │              │    启动 AM ─────→│             │            │      │
│    │              │               │             │            │      │
│    │              │               │ ④ 启动 AM   │            │      │
│    │              │               │  的 Container│            │      │
│    │              │               │             │            │      │
│    │              │               │←──── AM 启动成功          │      │
│    │              │               │             │            │      │
│    │ ⑤ 返回 AppID │               │             │            │      │
│    │←─────────────│               │             │            │      │
│    │              │               │             │            │      │
│    │              │               │    ⑥ AM 向 RM 注册       │      │
│    │              │               │             │──────────→│      │
│    │              │               │             │            │      │
│    │              │               │    ⑦ AM 向 RM 申请资源    │      │
│    │              │               │             │──────────→│      │
│    │              │               │             │            │      │
│    │              │               │    ⑧ RM 分配 Container    │      │
│    │              │               │             │←──────────│      │
│    │              │               │             │            │      │
│    │              │               │    ⑨ AM 与 NM 通信       │      │
│    │              │               │             │───────────→      │
│    │              │               │             │            │      │
│    │              │               │             │    ⑩ 启动    │      │
│    │              │               │             │    Map/Reduce│      │
│    │              │               │             │    Task      │      │
│    │              │               │             │            │      │
│    │              │               │             │ ⑪ 任务执行  │      │
│    │              │               │             │ (Map→Shuffle│      │
│    │              │               │             │  →Reduce)   │      │
│    │              │               │             │            │      │
│    │              │               │             │ ⑫ AM 通知  │      │
│    │              │               │             │    RM 完成  │      │
│    │              │               │             │──────────→│      │
│    │              │               │             │            │      │
│    │ ⑬ 查询结果   │               │             │            │      │
│    │─────────────→│               │             │            │      │
│    │              │               │             │            │      │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 实际操作：运行 WordCount 示例

Hadoop 自带了一个 WordCount 示例程序，我们可以用它来验证 YARN 是否正常工作。

```bash
# 步骤 1: 准备输入数据
mkdir -p /tmp/input
echo "hello world hadoop" > /tmp/input/file1.txt
echo "hello hdfs yarn mapreduce" > /tmp/input/file2.txt
echo "hadoop mapreduce yarn" > /tmp/input/file3.txt

# 步骤 2: 上传输入数据到 HDFS
hdfs dfs -mkdir -p /wordcount/input
hdfs dfs -put -f /tmp/input/* /wordcount/input/

# 步骤 3: 确认输出目录不存在（MapReduce 不允许输出目录已存在）
hdfs dfs -rm -r /wordcount/output 2>/dev/null

# 步骤 4: 提交 MapReduce 作业到 YARN
hadoop jar \
    $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar \
    wordcount \
    /wordcount/input \
    /wordcount/output

# 预期输出:
# 2026-04-15 10:00:00 INFO client.RMProxy: Connecting to ResourceManager at localhost/127.0.0.1:8032
# 2026-04-15 10:00:01 INFO mapred.JobClient: job_1713138000000_0001
# 2026-04-15 10:00:05 INFO mapred.JobClient: map 100% reduce 0%
# 2026-04-15 10:00:10 INFO mapred.JobClient: map 100% reduce 100%
# 2026-04-15 10:00:11 INFO mapred.JobClient: Job job_1713138000000_0001 completed successfully

# 步骤 5: 查看输出结果
hdfs dfs -cat /wordcount/output/*
# hadoop  2
# hdfs    1
# hello   2
# mapreduce 2
# world   1
# yarn    2
```

### 5.3 作业执行过程中的关键日志

```bash
# 查看 YARN 应用列表
yarn application -list
# application_1713138000000_0001  wordcount  user  RM    RUNNING  UNDEFINED  100%

# 查看 YARN 应用状态
yarn application -status application_1713138000000_0001

# 查看作业的 Map/Reduce 任务详情
yarn application -appStates ALL -list

# 杀死作业
yarn application -kill application_1713138000000_0001
```

## 六、Web UI 使用

### 6.1 ResourceManager Web UI

打开浏览器访问 http://localhost:8088

```
┌──────────────────────────────────────────────────────────────────┐
│              ResourceManager Web UI (http://localhost:8088)       │
│                                                                  │
│  ┌─ Cluster 信息 ────────────────────────────────────────────┐  │
│  │                                                            │  │
│  │  Cluster ID: xxx                                           │  │
│  │  Started on: Tue Apr 15 10:00:00 CST 2026                 │  │
│  │  Hadoop Version: 3.4.2                                    │  │
│  │                                                            │  │
│  │  Metrics:                                                  │  │
│  │  ├── Apps Submitted: 1                                    │  │
│  │  ├── Apps Running: 0                                      │  │
│  │  ├── Apps Pending: 0                                      │  │
│  │  ├── Apps Completed: 1                                    │  │
│  │  ├── Apps Killed: 0                                       │  │
│  │  └── Apps Failed: 0                                       │  │
│  │                                                            │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌─ Nodes ───────────────────────────────────────────────────┐  │
│  │                                                            │  │
│  │  Node Address      | State   | Total VMem | Used VMem     │  │
│  │  ─────────────────────────────────────────────            │  │
│  │  localhost:12345   | RUNNING | 8192 MB   | 2048 MB        │  │
│  │                                                            │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌─ Scheduler ───────────────────────────────────────────────┐  │
│  │                                                            │  │
│  │  Queue: root                                               │  │
│  │  ├── Capacity: 100%                                        │  │
│  │  ├── Used Capacity: 0%                                     │  │
│  │  ├── Max Capacity: 100%                                    │  │
│  │  └── Number of Applications: 0                             │  │
│  │                                                            │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  左侧导航栏:                                                     │
│  ├── About          集群信息                                     │
│  ├── Cluster        集群节点和队列信息                           │
│  │   ├── Nodes      所有 NodeManager                           │
│  │   └── Scheduler  调度器队列信息                              │
│  ├── Applications   所有应用列表                                 │
│  ├── Tools          工具                                        │
│  │   └── Conf       配置信息                                    │
│  └── Metrics        集群指标                                     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 6.2 NodeManager Web UI

打开浏览器访问 http://localhost:8042

可以查看：
- 该 NodeManager 的资源使用情况
- 正在运行的 Container 列表
- Container 的日志
- 节点健康状态

### 6.3 常用 yarn 命令

```bash
# 查看集群节点状态
yarn node -list

# 查看应用列表
yarn application -list
yarn application -appStates ALL -list  # 包括已完成的

# 查看应用详情
yarn application -status <application_id>

# 查看应用的尝试信息
yarn applicationattempt -list <application_id>

# 查看容器列表
yarn container -list <application_attempt_id>

# 杀死应用
yarn application -kill <application_id>

# 查看队列信息
yarn queue -status root
```

## 七、YARN 调度器简介

YARN 内置了三种调度器，可以通过配置选择使用：

```
┌──────────────────────────────────────────────────────────────┐
│              YARN 调度器对比                                   │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  FIFO Scheduler (先进先出)                                   │
│  ├── 最简单的调度器                                         │
│  ├── 按提交顺序执行                                         │
│  └── 不支持多队列，适合单用户环境                            │
│                                                              │
│  Capacity Scheduler (容量调度器) — 默认                     │
│  ├── 支持多队列，每个队列有容量保证                          │
│  ├── 支持层级队列（父子关系）                                │
│  ├── 支持用户级别限制                                       │
│  └── 适合多用户/多团队共享集群                              │
│                                                              │
│  Fair Scheduler (公平调度器)                                 │
│  ├── 所有应用平均分配资源                                    │
│  ├── 支持抢占（preemption）                                  │
│  ├── 动态调整资源分配                                       │
│  └── 适合多用户、任务量差异大的环境                         │
│                                                              │
│  默认使用 Capacity Scheduler，大多数场景不需要修改。          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 八、总结

```
┌────────────────────────────────────────────────────────────────┐
│                    本篇要点总结                                  │
│                                                                │
│  YARN 解决了什么问题:                                          │
│  将资源管理和任务调度分离，使 Hadoop 成为通用计算平台            │
│                                                                │
│  核心组件:                                                     │
│  RM (全局资源管理) → AM (单作业调度) → NM (单机资源管理)      │
│  Container = CPU + 内存的逻辑封装                             │
│                                                                │
│  作业提交流程:                                                 │
│  Client → RM → NM启动AM → AM申请资源 → NM启动Task → 完成     │
│                                                                │
│  关键配置:                                                     │
│  yarn.nodemanager.aux-services = mapreduce_shuffle (必须!)     │
│  yarn.nodemanager.resource.memory-mb = 物理内存 - OS开销       │
│                                                                │
│  Web UI:                                                       │
│  RM: http://localhost:8088                                    │
│  NM: http://localhost:8042                                    │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

> **上一篇：[04-读写流程详解](04-读写流程详解.md) — HDFS 读写流程深入**
>
> **下一篇：[06-MapReduce编程模型](06-MapReduce编程模型.md) — MapReduce 编程模型**
