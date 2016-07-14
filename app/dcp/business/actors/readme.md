###Actros 模块
actors 主要实现系统的actor框架，包括各种处理业务的actor。

####CodeMapActor
CodeMapActor 主要从fqueue上拿前端推过来的产品、标签等名字跟编码对应的表，这个表称为code map。
fqueue 上的code map 是json的，CodeMapActor 解析这个json后将信息缓存到本地，然后发送给DcpActor进行更新。

####DcpActor
DcpActor 主要负责清洗数据，将清洗后的数据放到一个buffer（ArrayBuffer）里，然后发送给其他跟外部系统打交道的Actor。
如EsjActor等

####EsjActor
EsjActor 主要负责将DcpActor清洗的数据进行处理后，发送给fqueue。然后ESJ系统会使用这些数据。

####FqueueActor
FqueueActor 主要负责初始化和监测FqueueClientActor。

####FqueueClientActor
FqueueClientActor 主要负责访问fqueue，包括存储数据。

####FqueueToolActor
FqueueToolActor 主要连接多个FqueueCliebtActor和多个DcpActor，做数据中转器。
FqueueToolActor 定时向FqueueClientActor 发起get请求，然后将收到的信息利用RR算法（轮询）转发给各个DcpActor。

####HbaseActor
HbaseActor将DcpActor清洗后的数据处理成Hbase需要的格式后，存储到hbase上。

####HdfsActor
HdfsActor定时将DcpActor清洗后的数据处理成hdfs需要的格式后，存储到hdfs上。

####LogerActor
LogerActor负责系统loger的接收与写入。

####OryxActor
OryxActor负责将DcpActor清洗后的数据进行处理后，使用http post 方法发送到oryx serving。