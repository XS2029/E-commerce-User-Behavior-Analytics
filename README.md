# 电商用户行为分析平台

## 项目简介
本项目是一个基于 Hadoop 生态的大数据离线分析平台，模拟电商用户行为日志，通过数据采集、清洗、存储、分析全流程，最终实现 RFM 用户价值分层。

## 技术栈
- **操作系统**：Ubuntu 22.04 (VMware 虚拟机)
- **大数据组件**：Hadoop (HDFS + YARN)、Hive、HBase、Spark
- **开发语言**：Python (数据模拟)、Scala (Spark 分析)、SQL (Hive)
- **数据库**：MySQL (Hive Metastore 及结果存储)

## 系统架构
![架构图](images/architecture.png)

## 数据规模
- 模拟生成 **100 万条**用户行为日志，包含 1000 个用户、500 个商品，4 种事件类型（view/click/purchase/add_to_cart）。
- HDFS 存储原始日志约 **110MB**。
- Hive 明细表 `dwd_user_action` 记录数 **~100 万**。

## 核心实现
1. **数据生成**：Python 脚本模拟实时用户行为，生成 JSON 格式日志。
2. **数据存储**：日志上传至 HDFS，Hive 创建外部表 `ods_user_log` 关联原始数据。
3. **数据清洗**：使用 Hive 内置函数 `get_json_object` 解析 JSON，构建明细表 `dwd_user_action`。
4. **维度数据**：在 HBase 中创建用户维度表 `dim_user`，存储用户属性（性别、年龄等）。
5. **Hive 与 HBase 集成**：通过 Hive 映射 HBase 表，实现事实表与维度表的关联查询。
6. **RFM 分析**：使用 Spark SQL 对购买记录进行聚合，计算最近一次消费（Recency）、消费频率（Frequency）、消费金额（Monetary），并基于评分规则划分用户价值等级（高/中/低）。
7. **结果存储**：将 RFM 结果写入 MySQL，供业务系统查询。

## 如何运行
1. 启动 Hadoop、HBase、MySQL 服务。
2. 运行 Python 脚本生成数据并上传 HDFS。
3. 执行 Hive SQL 创建表并进行 ETL。
4. 启动 Spark Shell 执行 RFM 分析并写入 MySQL。

## 项目亮点
- 完整搭建大数据离线分析全流程，涵盖 Hadoop、Hive、HBase、Spark 等组件。
- 打通 Hive 与 HBase，实现跨系统数据关联。
- 基于百万级数据实现 RFM 模型分析，并针对数据倾斜进行优化。
- 结果落地 MySQL，贴近真实业务场景。

## 联系方式
- GitHub：[@yourusername](https://github.com/yourusername)
- 邮箱：your.email@example.com
