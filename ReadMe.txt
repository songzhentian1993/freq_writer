kafka中读取展现数据，按ip+campagin_id和idfa+campaign_id维度分别统计展现频次。

实现逻辑：
  1. 从kafka中读取展现数据
  2. 初始化cache，从hbase中读取数据初始化cache
  3. 按adx+bid_id+ad_id+imp_id对展现数据去重(借助cache去重)
  4. 按ip+campagin_id和idfa+campaign_id维度分别统计展现频次(cache中计算且hbase做数据持久化)

执行：nohup ./freq_writer_supervise . &

