![logo](https://raw.github.com/alibaba/oceanbase/oceanbase_0.3/doc/%E5%9B%BE%E7%89%87%E5%A4%B9/logo.jpg)
  <p align=“right”>[English Version](https://github.com/alibaba/oceanbase/wiki/Oceanbase)</p>

OceanBase是阿里集团研发的可扩展的关系数据库，实现了数千亿条记录、数百TB数据上的跨行跨表事务，截止到2012年8月为止，支持了收藏夹、直通车报表、天猫评价等OLTP和OLAP在线业务，线上数据量已经超过一千亿条。

从模块划分的角度看，OceanBase可以划分为四个模块：主控服务器RootServer、更新服务器UpdateServer、基准数据服务器ChunkServer以及合并服务器MergeServer。OceanBase系统内部按照时间线将数据划分为基准数据和增量数据，基准数据是只读的，所有的修改更新到增量数据中，系统内部通过合并操作定期将增量数据融合到基准数据中。

<h1>发行日志</h1>
2013/04/28，整理提交了V0.4.1代码和相关技术文档。  
2013/03/04，整理提交了V0.3.1代码和相关技术文档。

<h1>0.4.1版本特性</h1>

- 使用libeasy网络框架代替了原来的tbnet，实现更高的网络处理性能
- 全面支持mysql协议（支持所有兼容libmysql库的driver，包含终端客户端mysql程序等）
- 支持SQL的客户端库
  - 提供了兼容JDBC的obdatasource库，实现OB集群内负载均衡策略
  - 提供了二进制兼容libmysql的libobsql库，实现OB集群内负载均衡策略
- 全面支持SQL
  - DML语句：select, insert, replace, delete, update
       - 支持select的大部分常用语法，包括任意复杂单表查询、集合交并差操作、受限的join、受限的子查询等
       - 支持的数据类型包括int, varchar, timestamp, float, double等
  - DDL语句：create table, drop table
  - 支持SQL事务
       - Start transaction, commit, rollback
       - SELECT FOR UPDATE
       - 实现了READ-COMMITED隔离级别的MVCC并发控制
  - 用户权限控制  
       - Create user, drop user, rename user, alter user, set password等
       - Grant, revoke等
  - 服务器端Prepared statement
       - 支持SQL语句prepare, execute, drop prepare等
       - 支持兼容mysql二进制协议的prepared statement
  - 其他语句
       - 通过set语句支持用户自定义变量
       - Show tables, show variables, show grants, show warnings, show columns, describe等
       - Explain
- 易用性改进
  - 引入了内部表机制，很多SQL功能基于内部表实现
       - Schema使用内部表管理，废除了schema配置文件
       - 引入了内部trigger通知机制
       - OB自身的配置使用内部表管理
  - 自监控
       - OB自身的监控信息可以通过查询内部表获得

 

<h1>资源列表</h1>
* [OceanBase架构](https://github.com/alibaba/oceanbase/wiki/OceanBase%E6%9E%B6%E6%9E%84%E4%BB%8B%E7%BB%8D%E5%85%A8%E6%96%87)
* [OceanBase安装部署](https://github.com/alibaba/oceanbase/wiki/OceanBase-Quick-Start-From-Sourcecode)
* [OceanBase客户端使用指南](https://github.com/alibaba/oceanbase/wiki/%E5%AE%A2%E6%88%B7%E7%AB%AF)
* [ChunkServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/chunkserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [mergeServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/mergeserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [rootServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/rootserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [updateServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/updateserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [运维文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/%E4%BD%BF%E7%94%A8%E8%BF%90%E7%BB%B4)
* [OceanBase淘宝开源站](http://oceanbase.taobao.org/)
* [Project Plan](https://github.com/alibaba/oceanbase/wiki/Project-Plan) 
<br>
