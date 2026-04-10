#!/usr/bin/env python3
"""ob-lens: OceanBase Comparator 可视化报告工具

用法:
    python3 ob_lens.py                    # 为最新运行生成 HTML 报告
    python3 ob_lens.py --run-id 20260301_093300  # 为指定运行生成报告
    python3 ob_lens.py --all              # 为所有运行生成报告
    python3 ob_lens.py --reports-dir /path/to/main_reports  # 指定报告目录
"""
import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="ob-lens: Oracle→OceanBase 迁移校验可视化报告工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--reports-dir",
        default="main_reports",
        help="main_reports 目录路径（默认: main_reports）",
    )
    parser.add_argument(
        "--fixup-dir",
        default="fixup_scripts",
        help="fixup_scripts 目录路径（默认: fixup_scripts）",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="指定运行 ID（如 20260301_093300），默认处理最新运行",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="为所有历史运行生成报告",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="输出 HTML 文件路径（默认: 与运行目录同位置）",
    )
    args = parser.parse_args()

    reports_dir = Path(args.reports_dir)
    fixup_dir = Path(args.fixup_dir)

    if not reports_dir.exists():
        print(f"错误：报告目录不存在: {reports_dir}", file=sys.stderr)
        return 1

    from ob_lens.report_builder import build_report, find_runs

    runs = find_runs(reports_dir)
    if not runs:
        print(f"错误：{reports_dir} 下没有找到任何运行目录", file=sys.stderr)
        return 1

    if args.all:
        target_runs = runs
    elif args.run_id:
        target_runs = [r for r in runs if r.name == f"run_{args.run_id}"]
        if not target_runs:
            print(f"错误：找不到运行 {args.run_id}", file=sys.stderr)
            return 1
    else:
        target_runs = [runs[-1]]  # 最新（按目录名排序后最后一个）

    for run_dir in target_runs:
        output_path = Path(args.output) if args.output and len(target_runs) == 1 else None
        html_path = build_report(run_dir, fixup_dir, output_path)
        print(f"✓ 报告已生成: {html_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
