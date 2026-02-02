## 1. Implementation
- [x] 1.1 视图兼容性：检测 X$ 依赖并支持用户自建例外
- [x] 1.2 视图 DDL 清洗：移除 FORCE 关键字
- [x] 1.3 视图权限拆分：生成 view_prereq_grants / view_post_grants
- [x] 1.4 视图权限顺序：run_fixup 执行顺序包含新目录
- [x] 1.5 视图授权报告与提示更新（fixup 目录提示）

## 2. Tests
- [x] 2.1 X$ 依赖识别/例外单测
- [x] 2.2 FORCE 清洗单测
- [x] 2.3 视图前/后置授权拆分单测

## 3. Docs
- [x] 3.1 更新 TECHNICAL_SPECIFICATION / CHANGELOG / README 提示
