## 1. Implementation
- [ ] 1.1 调整 LONG-only 黑名单阻断判断为“无论目标端是否存在均不阻断”
- [ ] 1.2 保留 LONG 转换校验逻辑，仅用于 blacklist_tables 报告
- [ ] 1.3 更新报告说明：LONG 黑名单不阻断依赖（不区分表是否存在）

## 2. Tests
- [ ] 2.1 LONG 表存在但列类型未转换：依赖不阻断，表列差异仍提示
- [ ] 2.2 LONG 表缺失：依赖不阻断，缺失表进入 fixup
- [ ] 2.3 LONG 表存在且列已转换：依赖不阻断，黑名单报告状态 VERIFIED
