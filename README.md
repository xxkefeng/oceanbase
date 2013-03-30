![logo](https://raw.github.com/alibaba/oceanbase/master/doc/%E5%9B%BE%E7%89%87%E5%A4%B9/logo.jpg)
  [English Version](https://github.com/alibaba/oceanbase/wiki/Oceanbase)

OceanBase是阿里集团研发的可扩展的关系数据库，实现了数千亿条记录、数百TB数据上的跨行跨表事务，截止到2012年8月为止，支持了收藏夹、直通车报表、天猫评价等OLTP和OLAP在线业务，线上数据量已经超过一千亿条。

从模块划分的角度看，OceanBase可以划分为四个模块：主控服务器RootServer、更新服务器UpdateServer、基准数据服务器ChunkServer以及合并服务器MergeServer。OceanBase系统内部按照时间线将数据划分为基准数据和增量数据，基准数据是只读的，所有的修改更新到增量数据中，系统内部通过合并操作定期将增量数据融合到基准数据中。

<h1>发行日志</h1>
2013/03/04，整理提交了V0.3.1代码和相关技术文档。

<h1>特性</h1>
* 透明化对动态数据的访问，作为OB集群的数据计算代理层；  
* 应用事件驱动模型，将跨tablet的查询并发执行降低OLAP请求的延迟；  
* 使用新负载均衡算法，消除不均衡访问增强系统整体QPS吞吐量；  
* 对大请求支持流式访问接口，提升OLAP大请求的数据吞吐；  
* 支持复杂表达式过滤条件，支持Sum,Count等聚合函数，支持GroupBy、Orderby等运算符；  
* CS旁路导入    
* tablet合并  
* 线上版本平滑升级(lsyncserver)  
* 去除对HA的依赖，由rootserver选主  
* 实现挂载多台备机  
* 实现准实时备份的主备集群  
* 增加hash+btree双索引机制  
* 优化memtable内存占用  
* 新的负载均衡策略  

<h1>资源列表</h1>
* [OceanBase介绍](https://github.com/alibaba/oceanbase/wiki)
* [OceanBase架构](https://github.com/alibaba/oceanbase/wiki/OceanBase%E6%9E%B6%E6%9E%84%E4%BB%8B%E7%BB%8D%E5%85%A8%E6%96%87)
* [OceanBase安装部署文档](https://github.com/alibaba/oceanbase/wiki/OceanBase-Quick-Start-From-Sourcecode)
* [ChunkServer设计文档](https://github.com/alibaba/oceanbase/tree/master/doc/chunkserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [mergeServer设计文档](https://github.com/alibaba/oceanbase/tree/master/doc/mergeserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [rootServer设计文档](https://github.com/alibaba/oceanbase/tree/master/doc/rootserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [updateServer设计文档](https://github.com/alibaba/oceanbase/tree/master/doc/updateserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [运维文档](https://github.com/alibaba/oceanbase/tree/master/doc/%E4%BD%BF%E7%94%A8%E8%BF%90%E7%BB%B4)
* [OceanBase淘宝开源站](http://oceanbase.taobao.org/)
* [Project Plan](https://github.com/alibaba/oceanbase/wiki/Project-Plan) 
<br>
