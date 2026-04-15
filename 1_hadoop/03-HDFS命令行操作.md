# 03 - HDFS Shell 命令大全

## 一、HDFS Shell 概述

HDFS Shell 是与 HDFS 交互最常用的方式。它的命令风格与 Linux Shell 非常相似，学习成本极低。

### 命令格式

```bash
hdfs dfs [选项] <命令> [参数]
# 或
hadoop fs [选项] <命令> [参数]
```

两者区别：
- `hdfs dfs`：专门用于操作 HDFS
- `hadoop fs`：可以操作多种文件系统（HDFS、本地文件系统、S3 等）

```
┌──────────────────────────────────────────────────────────────┐
│                HDFS Shell 命令分类                            │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  文件操作    ls, cat, tail, touchz, appendToFile             │
│  目录操作    mkdir, rmdir, du, df, count                     │
│  上传下载    put, get, copyToLocal, moveFromLocal            │
│  复制移动    cp, mv                                           │
│  权限管理    chmod, chown, chgrp                             │
│  状态检查    test, stat, fsck                                 │
│  其他        trash, expunge, setrep, getfacl, setfacl         │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 二、常用命令详解

### 2.1 ls — 列出文件和目录

```bash
# 语法
hdfs dfs -ls <路径>
hdfs dfs -ls [-R] [-d] [-h] <路径>

# 参数说明
# -R    递归列出所有子目录和文件
# -d    只列出目录本身，不列出内容
# -h    人类可读的文件大小（KB, MB, GB）

# 示例
hdfs dfs -ls /
# drwxr-xr-x   - user group          0 2026-04-15 10:00 /tmp
# drwxr-xr-x   - user group          0 2026-04-15 10:00 /user

hdfs dfs -ls -R /user
# 递归列出 /user 下所有文件

hdfs dfs -ls -h /user/data
# -rw-r--r--   3 user group    1.2 G 2026-04-15 10:00 access.log
```

输出格式说明：
```
-rw-r--r--   3 user group  134217728 2026-04-15 10:00 /user/data/access.log
  │          │ │    │        │         │              │
  │          │ │    │        │         │              └── 文件路径
  │          │ │    │        │         └── 最后修改时间
  │          │ │    │        └── 文件大小（字节）
  │          │ │    └── 所属组
  │          │ └── 所属用户
  │          └── 副本数量（目录显示 -）
  └── 权限（rwx: 读写执行）
```

### 2.2 mkdir — 创建目录

```bash
# 语法
hdfs dfs -mkdir [-p] <路径>

# 参数说明
# -p    递归创建父目录（类似 Linux 的 mkdir -p）

# 示例
hdfs dfs -mkdir /user/你的用户名/data
# 成功

hdfs dfs -mkdir /user/你的用户名/data/files/2026
# 失败！父目录 files 不存在

hdfs dfs -mkdir -p /user/你的用户名/data/files/2026
# 成功！-p 自动创建中间目录
```

### 2.3 put — 上传文件到 HDFS

```bash
# 语法
hdfs dfs -put [-f] [-p] <本地源> <HDFS目标>

# 参数说明
# -f    如果目标已存在，覆盖它
# -p    保留文件的访问时间、修改时间、权限和所有权

# 示例
# 上传单个文件
hdfs dfs -put /tmp/test.txt /user/你的用户名/data/

# 上传并覆盖
hdfs dfs -put -f /tmp/test.txt /user/你的用户名/data/test.txt

# 上传整个目录
hdfs dfs -put /local/data/ /user/你的用户名/data/

# 从标准输入上传
echo "Hello from stdin" | hdfs dfs -put - /user/你的用户名/data/stdin.txt
```

```
┌──────────────────────────────────────────────────────────┐
│              put 的工作原理                                │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  put 命令执行时发生了什么:                                │
│                                                          │
│  1. 客户端连接 NameNode                                   │
│     → 请求创建文件                                        │
│     → NameNode 检查权限和目录是否存在                     │
│                                                          │
│  2. 客户端将文件切分成 Block                              │
│     → 根据副本策略选择 DataNode                           │
│                                                          │
│  3. 客户端直接与 DataNode 通信                            │
│     → 将数据以 Block 为单位写入 DataNode                  │
│                                                          │
│  4. 完成后通知 NameNode                                   │
│     → NameNode 更新元数据                                │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 2.4 get — 从 HDFS 下载文件

```bash
# 语法
hdfs dfs -get [-f] [-p] <HDFS源> <本地目标>

# 参数说明
# -f    如果本地文件已存在，覆盖它
# -p    保留文件属性

# 示例
hdfs dfs -get /user/你的用户名/data/test.txt /tmp/downloaded_test.txt

# 下载整个目录
hdfs dfs -get /user/你的用户名/data /tmp/data_backup
```

### 2.5 cat — 查看文件内容

```bash
# 语法
hdfs dfs -cat <文件路径>

# 示例
hdfs dfs -cat /user/你的用户名/data/test.txt
# 输出文件全部内容

# 查看多个文件
hdfs dfs -cat /user/你的用户名/data/file1.txt /user/你的用户名/data/file2.txt

# 查看并使用管道
hdfs dfs -cat /user/你的用户名/data/access.log | head -20
hdfs dfs -cat /user/你的用户名/data/access.log | grep "404"
```

### 2.6 rm — 删除文件或目录

```bash
# 语法
hdfs dfs -rm [-r] [-f] [-skipTrash] <路径>

# 参数说明
# -r           递归删除目录及其所有内容
# -f           忽略不存在的文件，不报错
# -skipTrash   跳过回收站，直接永久删除

# 示例
# 删除文件
hdfs dfs -rm /user/你的用户名/data/test.txt

# 删除目录
hdfs dfs -rm -r /user/你的用户名/data/old_files

# 永久删除（跳过回收站）
hdfs dfs -rm -r -skipTrash /user/你的用户名/data/temp

# 强制删除（不存在的文件不报错）
hdfs dfs -rm -f /user/你的用户名/data/nonexistent.txt
```

### 2.7 cp — 复制

```bash
# 语法
hdfs dfs -cp [-f] <HDFS源> <HDFS目标>

# 参数说明
# -f    如果目标已存在，覆盖它

# 示例
hdfs dfs -cp /user/你的用户名/data/test.txt /user/你的用户名/data/test_backup.txt

# 复制目录
hdfs dfs -cp /user/你的用户名/data/files /user/你的用户名/data/files_backup
```

### 2.8 mv — 移动/重命名

```bash
# 语法
hdfs dfs -mv <HDFS源> <HDFS目标>

# 示例
# 重命名文件
hdfs dfs -mv /user/你的用户名/data/old_name.txt /user/你的用户名/data/new_name.txt

# 移动文件到另一个目录
hdfs dfs -mv /user/你的用户名/data/test.txt /user/你的用户名/data/files/

# 移动多个文件到目录
hdfs dfs -mv /user/你的用户名/data/file1.txt /user/你的用户名/data/file2.txt /user/你的用户名/data/files/
```

### 2.9 chmod — 修改权限

```bash
# 语法
hdfs dfs -chmod [-R] <权限模式> <路径>

# 参数说明
# -R    递归修改

# 示例
# 八进制模式
hdfs dfs -chmod 755 /user/你的用户名/data/script.sh

# 符号模式
hdfs dfs -chmod u+x /user/你的用户名/data/script.sh      # 给用户添加执行权限
hdfs dfs -chmod g-w /user/你的用户名/data/data           # 去掉组的写权限
hdfs dfs -chmod o=r /user/你的用户名/data/README         # 其他人只读

# 递归修改目录权限
hdfs dfs -chmod -R 755 /user/你的用户名/data
```

### 2.10 chown — 修改所有者

```bash
# 语法
hdfs dfs -chown [-R] <用户>:<组> <路径>

# 示例
hdfs dfs -chown admin:admin /user/你的用户名/data
hdfs dfs -chown -R admin:admin /user/你的用户名/data
hdfs dfs -chown :data_team /user/你的用户名/data/data   # 只修改组
```

### 2.11 du — 显示文件/目录大小

```bash
# 语法
hdfs dfs -du [-s] [-h] [-v] <路径>

# 参数说明
# -s    显示汇总大小
# -h    人类可读格式
# -v    显示详细信息（文件名 + 大小）

# 示例
hdfs dfs -du /user/你的用户名/data
# 134217728  134217728  /user/你的用户名/data/access.log
#  第一个数字: 文件大小（字节）
#  第二个数字: 副本占用的总空间（字节）

hdfs dfs -du -s -h /user/你的用户名/data
# 1.0 G  3.0 G  /user/你的用户名/data
#  第一个数字: 实际文件大小
#  第二个数字: 所有副本的总大小
```

### 2.12 df — 显示文件系统使用情况

```bash
# 语法
hdfs dfs -df [-h]

# 示例
hdfs dfs -df -h
# Filesystem          Size     Used    Available    Use%
# hdfs://localhost:8020  500.0 G  10.0 G    490.0 G    2%
```

### 2.13 count — 统计文件和目录数量

```bash
# 语法
hdfs dfs -count [-q] [-h] <路径>

# 参数说明
# -q    显示配额信息
# -h    人类可读格式

# 示例
hdfs dfs -count /user/你的用户名/data
# 5        8        134217728 /user/你的用户名/data
#  │        │            │
#  │        │            └── 目录内容总大小（字节）
#  │        └── 文件数量
#  └── 目录数量

hdfs dfs -count -q -h /user/你的用户名/data
# none             inf            100.0 G        5        8        128.0 M /user/你的用户名/data
#  │                │               │
#  │                │               └── 目录配额
#  │                └── 空间配额
#  └── 名称配额
```

### 2.14 tail — 查看文件末尾

```bash
# 语法
hdfs dfs -tail [-f] <文件路径>

# 参数说明
# -f    持续追踪文件末尾（类似 Linux 的 tail -f）

# 示例
hdfs dfs -tail /user/你的用户名/data/access.log
# 显示文件最后 1KB 的内容

hdfs dfs -tail -f /user/你的用户名/data/access.log
# 持续显示新追加的内容（适合监控日志）
# 按 Ctrl+C 退出
```

### 2.15 test — 检查文件状态

```bash
# 语法
hdfs dfs -test -[defsz] <路径>

# 参数说明
# -d    路径是否是目录
# -e    路径是否存在
# -f    路径是否是文件
# -s    文件是否非空
# -z    文件是否为空

# 示例
hdfs dfs -test -d /user/你的用户名/data && echo "是目录" || echo "不是目录"
hdfs dfs -test -e /user/你的用户名/data/test.txt && echo "文件存在" || echo "文件不存在"
hdfs dfs -test -f /user/你的用户名/data/test.txt && echo "是文件" || echo "不是文件"

# 在脚本中使用
if hdfs dfs -test -e /user/data/input; then
    echo "输入目录存在，开始处理"
    # 执行 MapReduce 作业
else
    echo "输入目录不存在"
    exit 1
fi
```

## 三、其他常用命令

### 3.1 touchz — 创建空文件

```bash
hdfs dfs -touchz /user/你的用户名/data/.keep
# 注意: touchz 不能对已存在的文件使用（与 Linux touch 不同）
```

### 3.2 appendToFile — 追加内容到文件

```bash
# 追加本地文件内容到 HDFS 文件
hdfs dfs -appendToFile /tmp/new_data.txt /user/你的用户名/data/access.log

# 从标准输入追加
echo "new log entry" | hdfs dfs -appendToFile - /user/你的用户名/data/access.log

# 注意: 追加前必须确保 hdfs-site.xml 中以下配置为 true
# dfs.support.append = true（Hadoop 2.x）
# dfs.append.enabled = true（Hadoop 3.x，默认已开启）
```

### 3.3 setrep — 设置副本数量

```bash
# 语法
hdfs dfs -setrep [-R] <副本数> <路径>

# 示例
# 设置单个文件的副本数
hdfs dfs -setrep 2 /user/你的用户名/data/important_data.csv

# 递归设置整个目录的副本数
hdfs dfs -setrep -R 2 /user/你的用户名/data

# 注意: setrep 只影响新创建的副本，已有副本会在后台逐渐调整
```

### 3.4 stat — 查看文件详细信息

```bash
hdfs dfs -stat "%F %u:%g %b %n %y" /user/你的用户名/data/test.txt
# file user:group 134217728 /user/你的用户名/data/test.txt 2026-04-15 10:00:00

# 格式化选项:
# %F    文件类型（file/directory）
# %u    所有者
# %g    所属组
# %b    文件大小（字节）
# %n    文件名
# %y    最后修改时间
# %r    副本数量
```

### 3.5 moveFromLocal / copyFromLocal / moveToLocal / copyToLocal

```bash
# moveFromLocal: 移动本地文件到 HDFS（本地文件会被删除）
hdfs dfs -moveFromLocal /tmp/local_file.txt /user/你的用户名/data/

# copyFromLocal: 复制本地文件到 HDFS（等同于 put）
hdfs dfs -copyFromLocal /tmp/local_file.txt /user/你的用户名/data/

# moveToLocal: 从 HDFS 移动到本地（HDFS 文件会被删除）
# 注意: 这个命令在 Hadoop 3.x 中已被标记为废弃
# 建议使用 get + rm 代替

# copyToLocal: 从 HDFS 复制到本地（等同于 get）
hdfs dfs -copyToLocal /user/你的用户名/data/test.txt /tmp/
```

## 四、实战：创建数据平台项目目录结构

假设你的数据平台项目需要将日志、数据、配置文件存储到 HDFS，我们来创建一个完整的目录结构：

```bash
# 步骤 1: 创建基础目录结构
hdfs dfs -mkdir -p /data/{logs,data,config,backup,archive}

# 步骤 2: 创建日志目录的子目录
hdfs dfs -mkdir -p /data/logs/{access,error,application}

# 步骤 3: 创建数据目录的子目录
hdfs dfs -mkdir -p /data/data/{raw,processed,temp}

# 步骤 4: 创建备份的按月归档目录
hdfs dfs -mkdir -p /data/backup/2026/{04,05,06}

# 步骤 5: 查看完整的目录结构
hdfs dfs -ls -R /data
```

```
创建后的目录结构:
/data/
├── logs/
│   ├── access/
│   ├── error/
│   └── application/
├── data/
│   ├── raw/
│   ├── processed/
│   └── temp/
├── config/
├── backup/
│   └── 2026/
│       ├── 04/
│       ├── 05/
│       └── 06/
└── archive/
```

```bash
# 步骤 6: 上传一些示例数据
echo "2026-04-15 10:00:01 INFO  homepage visited" > /tmp/access.log
echo "2026-04-15 10:00:02 WARN  slow query detected" >> /tmp/access.log
echo "2026-04-15 10:00:03 ERROR database connection failed" > /tmp/error.log
echo "server.port=8080" > /tmp/application.conf

hdfs dfs -put /tmp/access.log /data/logs/access/
hdfs dfs -put /tmp/error.log /data/logs/error/
hdfs dfs -put /tmp/application.conf /data/config/

# 步骤 7: 验证
hdfs dfs -cat /data/logs/access/access.log
hdfs dfs -cat /data/logs/error/error.log
hdfs dfs -cat /data/config/application.conf

# 步骤 8: 查看空间使用情况
hdfs dfs -du -h /data
hdfs dfs -count -h /data

# 步骤 9: 下载备份
hdfs dfs -get /data/config/application.conf /tmp/downloaded_config.conf
```

## 五、HDFS 回收站（Trash）机制

### 5.1 什么是回收站？

当你在 `core-site.xml` 中配置了 `fs.trash.interval` 后，执行 `hdfs dfs -rm` 命令不会立即永久删除文件，而是将文件移到回收站。

```
┌──────────────────────────────────────────────────────────────┐
│                HDFS 回收站机制                                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  执行 hdfs dfs -rm /data/temp/data.txt 时:                  │
│                                                              │
│  1. 文件被移动到回收站目录                                    │
│     /user/你的用户名/.Trash/Current/data/temp/data.txt       │
│                                                              │
│  2. 回收站目录结构:                                          │
│     /user/你的用户名/                                        │
│     └── .Trash/                                             │
│         ├── Current/              ← 当前回收站              │
│         │   └── data/temp/data.txt                          │
│         └── 1466304000000/         ← 过期检查点目录          │
│             └── (已过期的文件会被移到这里，然后被清理)       │
│                                                              │
│  3. 文件在回收站保留 1440 分钟（24 小时）                    │
│     超时后，NameNode 的 Trash Emptier 线程会自动清理          │
│                                                              │
│  4. 检查间隔: fs.trash.checkpoint.interval（默认 0 = 使用   │
│     fs.trash.interval 的值）                                 │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 5.2 回收站相关操作

```bash
# 查看回收站内容
hdfs dfs -ls /user/你的用户名/.Trash/Current/

# 从回收站恢复文件
hdfs dfs -mv /user/你的用户名/.Trash/Current/data/temp/data.txt /data/temp/data.txt

# 清空回收站
hdfs dfs -expunge

# 永久删除（跳过回收站）
hdfs dfs -rm -skipTrash /data/temp/data.txt
```

## 六、常见错误排查

### 6.1 No such file or directory

```bash
# 错误
hdfs dfs -cat /data/nonexistent.txt
# ls: `/data/nonexistent.txt': No such file or directory

# 排查
hdfs dfs -ls /data/     # 确认文件名是否正确
hdfs dfs -ls -R /       # 从根目录搜索
```

### 6.2 Permission denied

```bash
# 错误
hdfs dfs -mkdir /restricted_dir
# mkdir: `/restricted_dir': Permission denied: user=your_user, access=WRITE, inode="/":user=superuser:supergroup:rwxr-xr-x

# 排查
hdfs dfs -ls /          # 查看根目录权限
# 如果权限不足，在 /user/你的用户名/ 下操作
hdfs dfs -mkdir /user/你的用户名/restricted_dir

# 或者关闭权限检查（仅学习环境！）
# 修改 hdfs-site.xml: dfs.permissions.enabled = false
```

### 6.3 put: File exists

```bash
# 错误
hdfs dfs -put /tmp/data.txt /data/data/raw/
# put: `/data/data/raw/data.txt': File exists

# 解决: 使用 -f 覆盖
hdfs dfs -put -f /tmp/data.txt /data/data/raw/
```

### 6.4 NameNode is in safe mode

```bash
# 错误
hdfs dfs -put /tmp/big_file.dat /data/data/
# put: Cannot create /data/data/big_file.dat. Name node is in safe mode.

# 原因: NameNode 启动后进入安全模式，此时不允许写操作
# 排查: 查看安全模式状态
hdfs dfsadmin -safemode get

# 等待自动退出（DataNode 块报告达到阈值后自动退出）
# 或者手动退出（生产环境不推荐！）
hdfs dfsadmin -safemode leave
```

### 6.5 Connection refused

```bash
# 错误
hdfs dfs -ls /
# ls: Call From xxx/127.0.0.1 to localhost:8020 failed on connection exception

# 排查步骤
# 1. 检查 NameNode 是否运行
jps | grep NameNode

# 2. 如果没有运行，启动 HDFS
start-dfs.sh

# 3. 如果已经在运行，检查端口
lsof -i :8020

# 4. 检查 core-site.xml 中的 fs.defaultFS 配置
cat $HADOOP_HOME/etc/hadoop/core-site.xml | grep fs.defaultFS
```

## 七、命令速查表

```
┌────────────────────────────────────────────────────────────────┐
│                  HDFS Shell 命令速查表                         │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  操作          命令                        示例                │
│  ─────────────────────────────────────────────────────         │
│  列出目录      hdfs dfs -ls [-R]       -ls /user/data         │
│  创建目录      hdfs dfs -mkdir [-p]    -mkdir -p /a/b/c       │
│  上传文件      hdfs dfs -put            -put local.txt /dir/   │
│  下载文件      hdfs dfs -get            -get /dir/file local/  │
│  查看内容      hdfs dfs -cat            -cat /dir/file         │
│  查看末尾      hdfs dfs -tail [-f]      -tail -f /dir/log     │
│  删除文件      hdfs dfs -rm [-r]        -rm -r /dir/old/      │
│  复制          hdfs dfs -cp             -cp /src /dst          │
│  移动          hdfs dfs -mv             -mv /src /dst          │
│  修改权限      hdfs dfs -chmod          -chmod 755 /dir       │
│  修改所有者    hdfs dfs -chown          -chown user:grp /dir  │
│  查看大小      hdfs dfs -du [-s] [-h]   -du -h /dir           │
│  磁盘使用      hdfs dfs -df [-h]        -df -h                │
│  统计数量      hdfs dfs -count          -count /dir           │
│  创建空文件    hdfs dfs -touchz         -touchz /dir/file     │
│  追加内容      hdfs dfs -appendToFile   -appendToFile l r     │
│  设置副本      hdfs dfs -setrep [-R]    -setrep 3 /dir        │
│  文件检查      hdfs dfs -test -[defsz]  -test -e /dir/file    │
│  空文件检查    hdfs dfs -test -z         -test -z /dir/file    │
│  查看详情      hdfs dfs -stat           -stat "%n %b" /file   │
│  清空回收站    hdfs dfs -expunge        -expunge              │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

> **上一篇：[02-环境搭建](02-环境搭建.md) — 环境搭建详解**
>
> **下一篇：[04-读写流程详解](04-读写流程详解.md) — HDFS 读写流程深入**
