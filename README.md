# aliyun-mc-topconsole-openapi

## Maven
```
<dependency>
    <groupId>com.aliyun</groupId>
    <artifactId>maxcompute20220104</artifactId>
    <version>3.3.0</version>
</dependency>
```

## Observation
### Job
[Job Info List](https://help.aliyun.com/zh/maxcompute/user-guide/api-maxcompute-2022-01-04-listjobinfos?spm=a2c4g.11186623.0.i1)

1. 展示指定时间范围(from, to)的作业列表。 
   - 只要作业的生命周期和指定的时间范围有交叉，均会展示。
   - 表示 from ~ to时间内执行结束的作业，以及在 to 时间点仍未结束的作业列表
2. 作业从submitted状态开始，经历running，到达terminated/cancelled/failed
3. 监控数据采用旁路流式处理后展示，有2～3分钟延时。
   - 作业的提交和运行在MaxCompute里不会有延时，二者并不是同一条链路。

[Job Snapshot List](https://help.aliyun.com/zh/maxcompute/user-guide/api-maxcompute-2022-01-04-listjobsnapshotinfos?spm=a2c4g.11186623.help-menu-27797.d_2_13_3_4_3.72ebfa6fk9piaI)

1. 展示指定时间的作业快照信息
   - 实现上，会根据指定的时间to, 按照(to - 180s, to)的范围找到最新的快照信息，并展示。 
2. 快照数据的采集频率为60s，因此不能保证所有作业的运行态信息均被覆盖。

[Job Info Detail with SubStatus](https://help.aliyun.com/zh/maxcompute/user-guide/api-maxcompute-2022-01-04-getjobinfo?spm=a2c4g.11186623.help-menu-27797.d_2_13_3_4_4.7c4a58167jLg1l)

1. 会额外展示作业的[subStatus](https://help.aliyun.com/zh/maxcompute/user-guide/use-logview-v2-0-to-view-job-information?spm=a2c4g.11186623.help-menu-27797.d_2_8_4_1.a168fa6fieGeqI&scm=20140722.H_183576._.OR_help-T_cn~zh-V_1#p-85o-v6s-gdg)信息。

### Quota
[Quota Usage Metric](https://help.aliyun.com/zh/maxcompute/user-guide/api-maxcompute-2022-01-04-getquotausage?spm=a2c4g.11186623.help-menu-27797.d_2_13_3_3_0.13017e5c9t8NBc&scm=20140722.H_2861470._.OR_help-T_cn~zh-V_1)

1. Quota的采集频率为60s，会根据时间范围进行自适应步长。
2. 具体的使用请参考官方文档。

### Tunnel
[Tunnel Usage Metric](https://help.aliyun.com/document_detail/2882199.html?spm=openapi-amp.newDocPublishment.0.0.63f5281fJpNpHn)

[Tunnel Usage Detail](https://help.aliyun.com/zh/maxcompute/user-guide/api-maxcompute-2022-01-04-querytunnelmetricdetail?spm=a2c4g.11186623.help-menu-27797.d_2_13_3_3_1.4e646635UNHGJW)

1. Tunnel的采集频率为60s，会根据时间范围进行自适应步长。
2. Tunnel的指标较多，逻辑较复杂，请参考官方文档。