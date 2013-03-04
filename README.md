OceanBase是阿里集团研发的可扩展的关系数据库，实现了数千亿条记录、数百TB数据上的跨行跨表事务，截止到2012年8月为止，支持了收藏夹、直通车报表、天猫评价等OLTP和OLAP在线业务，线上数据量已经超过一千亿条。

从模块划分的角度看，OceanBase可以划分为四个模块：主控服务器RootServer、更新服务器UpdateServer、基准数据服务器ChunkServer以及合并服务器MergeServer。OceanBase系统内部按照时间线将数据划分为基准数据和增量数据，基准数据是只读的，所有的修改更新到增量数据中，系统内部通过合并操作定期将增量数据融合到基准数据中。本章介绍OceanBase系统的设计思路和整体架构。

<h1>发行日志</h1>
2013/03/04，整理提交了V0.3.1代码和相关技术文档。
<h1>资源列表</h1>

*[OceanBase架构](https://github.com/xiusiyan/oceanbase/tree/master/doc/oceanbase%E6%9E%B6%E6%9E%84%E5%92%8C%E6%8E%A5%E5%8F%A3)
*[ChunkServer设计文档](https://github.com/xiusiyan/oceanbase/tree/master/doc/chunkserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
*[mergeServer设计文档](https://github.com/xiusiyan/oceanbase/tree/master/doc/mergeserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
*[rootServer设计文档](https://github.com/xiusiyan/oceanbase/tree/master/doc/rootserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
*[updateServer设计文档](https://github.com/xiusiyan/oceanbase/tree/master/doc/updateserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
*[运维文档](https://github.com/xiusiyan/oceanbase/tree/master/doc/%E4%BD%BF%E7%94%A8%E8%BF%90%E7%BB%B4)