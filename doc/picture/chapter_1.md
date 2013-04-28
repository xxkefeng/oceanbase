# 1 Basic Information #
This chapter introduces the basic information you need to know before installing OceanBase. </p>

----------
## 1.1 Overview ##
OceanBase is Alibaba Group's research and development of scalable, distributed relational database. The database allows multi-cross-table transaction with hundreds of billions of records, hundreds of Taobao data, and supports OLTP and OLAP online services, such as favorites, train statements, and Tianmao evaluation.<p>

The data in OceanBase can be divided into the baseline data and incremental data. The baseline data is read-only data, and the incremental data need to modify the updated data. OceanBase regularly merge incremental data to the baseline data.<p>

OceanBase database can be divided into the following four modules:<br>


- Root Server<br>
  Master server. It mainly provides cluster management, data distribution management and replica management.<p>

- Update Server<br>
   Update server. It's a written module in the cluster, and stores the incremental data.<p>

- Chunk Server<br>
   Baseline data server. It stores the Baseline data server, and provides data access service, regularly merging, and data distribution.<p>

- Merge Server<br>
   Merge server. It mainly provides protocol analysis, SQL parsing, forwards the request, the results of mergers and multi-table operations.<br>
   Listener is a special Merge Server in the OceanBase cluster. It queries the internal table of the cluster master and slave cluster traffic distribution and the all other address list of Merge Servers.<p>
## 1.2	Networking ##
<b>Figure 1-1</b> shows the OceanBase network.<p>
<b>Figure 1-1 OceanBase network</b><p>
![Alt text](/imgs/1_01.png)<p>

OceanBase can be deployed on a single server, or deployed separately. <b>Table 1-1</b> shows the component introduction when the OceanBase is deployed separately.<p>
<b>Table 1-1 Datebase Component</b><p>
<table width=1000 border="1" frame="all" rules="all">
   <tr>
      <td bgcolor="#B0B0B0" width="30%"><b>Component</b></td>
      <td bgcolor="#B0B0B0" width="70%"><b>Description</b></td>
   </tr>
   <tr>
      <td>Root Server</td>
      <td>Root Server can be deployed active and standby，strong data synchronization between active and standby. <br>Root Server and  Update Server is deployed on a server.</td>
   </tr>
   <tr>
      <td>Update Server</td>
      <td>Update Server can be deployed active and standby. You can configure different synchronization modes between active and standby.<br>UpdateServer and Root Server is deployed on a server.</td>
   </tr>
   <tr>
      <td>Chunk Server</td>
      <td>It stores OceanBase baseline data. The baseline data is typically stored in duplicate or triplicate。The Chunk Server can be deployed on cluster.</td>
   </tr>
   <tr>
      <td>Merge Server</td>
      <td>It merger UpdateServer and ChunkServer data. The Merge Server can be deployed on cluster. 
         <br>Listener is a special Merge Server in the OceanBase cluster, and deployed with the Root Server.</td>
   </tr>
   <tr>
      <td>Client</td>
      <td>The Client has the RootServer address list of multiple clusters, and sents the read or write poerations to different clusters according to the proportion of the Flow distribution in the cluster. </td>
   </tr>
</table>

## 1.3 Hardware&Software Requirements ##
The configuration requirements of OceanBase servers are as follows:<p>


- When the OceanBase is deployed on a single server, the server should be a 64-bit Linux system, with the 10GB memory and 100M disk space.<p>

- When the OceanBase is deployed separately, the hardware and software requirement is shown as <b>Table 1-2</b>. <p>
<b>Table 1-2 Server Requirements</b>
<table width=1000 border="1" frame="all" rules="all">
   <tr>
      <td bgcolor="#B0B0B0" width="25%"><b>Component</b></td>
      <td bgcolor="#B0B0B0" width="25%"><b>Operating system</b></td>
      <td bgcolor="#B0B0B0" width="25%"><b>Memory</b></td>
      <td bgcolor="#B0B0B0" width="25%"><b>Disk space</b></td>
   </tr>
   <tr>
      <td>Root Server/Listener</td>
      <td>Linux, 64-bit</td>
      <td>1GB</td>
      <td>20GB</td>
   </tr>
   <tr>
      <td>Update Server</td>
      <td>Linux, 64-bit</td>
      <td>3GB</td>
      <td>20GB</td>
   </tr>
   <tr>
      <td>Chunk Server</td>
      <td>Linux, 64-bit</td>
      <td>1GB</td>
      <td>20GB</td>
   </tr>
   <tr>
      <td>Merge Server</td>
      <td>Linux, 64-bit</td>
      <td>1GB</td>
      <td>20GB</td>
   </tr>
</table>