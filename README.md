#dcp 2.0
dcp 系统主要实现用户轨迹的清洗，统计，存储。
dcp 主要有几个模块，分别是： system、config、common、business。

###system
主要提供系统初始化、监控、恢复等功能。

###config
主要提供系统配置、动态配置等功能。

###common
一些公用的类、模块等。

###business
整个系统的框架、主要业务的实现。
又分为actor、codemap、db、service、statistics。
#####1、actor
实现系统框架的actor模块
#####2、codemap
定时更新codemap数据结构。
####3、db
实现hbase的管理、访问。
#####4、service
dcp系统数据清洗的业务逻辑的实现。
#####5、statistics
提供清洗过的数据的访客量、数据量的统计，供dsp使用。

感谢@[cwxtop](https://github.com/cwxtop)
