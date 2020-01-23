# 关键短语抽取-Keyphrase Extraction

关键短语抽取; keyphrase extraction

## 如何运行

1. 安装sbt
2. git clone git@github.com:iamxiatian/keyphrase.git
3. cd keyphrase
4. sbt compile
5. IntelliJ IDEA安装scala和sbt插件，然后以工程方式打开build.sbt

## 测试入口

关键短语抽取的测试类请参考：KeyphraseTest.scala

## 数据集

1. 基于2015年以来中文图情档CSSCI期刊论文的标题、摘要和关键词构成的数据集，存放在 data/paper_abstract.csv

## 系统设计

### HTTP服务

HTTP服务的入口为HttpServer类，该类会加载各个Restful API的处理类，
API的处理类以Route结尾，如关键词抽取为KeywordRoute.scala文件。

HttpServer服务采用了spark java实现，默认会优先响应API的拦截处理，
没有对应的API时，则会读取www目录下的文件。

