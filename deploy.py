#!/usr/bin/env python3
"""
YouTube 下载器 - 上线预处理脚本

功能：
1. 清理日志文件
2. 清理本地数据库文件（含 WAL/SHM）
3. 清理下载目录中的所有文件
4. 扫描项目结构并统计代码行数
5. 解析 Flask 路由（run.py 中的 @app.route）
6. 自动生成 CODE.md 项目文档
"""

import os
import sys
import re
import shutil
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent
LOG_DIR = PROJECT_ROOT / "log"
DOWNLOADS_DIR = PROJECT_ROOT / "downloads"
STATIC_DIR = PROJECT_ROOT / "static"
CODE_MD_FILE = PROJECT_ROOT / "CODE.md"
APP_ENTRY = PROJECT_ROOT / "run.py"

DB_FILES = [
    PROJECT_ROOT / "data.db",
    PROJECT_ROOT / "data.db-wal",
    PROJECT_ROOT / "data.db-shm",
]


def print_header(title):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_step(step_num, total, message):
    print(f"\n[{step_num}/{total}] {message}")


def confirm(message):
    response = input(f"\n{message} (y/n): ").strip().lower()
    return response in ('y', 'yes', '是')


def clean_logs():
    if not LOG_DIR.exists():
        print("  日志目录不存在，跳过")
        return 0

    log_files = [p for p in LOG_DIR.iterdir() if p.is_file()]
    if not log_files:
        print("  没有日志文件需要清理")
        return 0

    count = 0
    for log_file in log_files:
        try:
            log_file.unlink()
            print(f"  ✓ 已删除: {log_file.name}")
            count += 1
        except Exception as e:
            print(f"  ✗ 删除失败 {log_file.name}: {e}")
    return count


def clean_database():
    cleaned = 0
    for db_file in DB_FILES:
        if db_file.exists():
            try:
                db_file.unlink()
                print(f"  ✓ 已删除: {db_file.name}")
                cleaned += 1
            except Exception as e:
                print(f"  ✗ 删除失败 {db_file.name}: {e}")
    if cleaned == 0:
        print("  数据库文件不存在，跳过")
    return cleaned


def clean_downloads():
    if not DOWNLOADS_DIR.exists():
        print("  下载目录不存在，跳过")
        return 0

    all_files = list(DOWNLOADS_DIR.iterdir())
    if not all_files:
        print("  下载目录为空，无需清理")
        return 0

    count = 0
    for file_path in all_files:
        try:
            if file_path.is_file() or file_path.is_symlink():
                file_path.unlink()
                print(f"  ✓ 已删除: {file_path.name}")
                count += 1
            elif file_path.is_dir():
                shutil.rmtree(file_path)
                print(f"  ✓ 已删除目录: {file_path.name}")
                count += 1
        except Exception as e:
            print(f"  ✗ 删除失败 {file_path.name}: {e}")

    print(f"  共清理 {count} 个文件/目录")
    return count


def count_lines(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return len(f.readlines())
    except Exception:
        return 0


def get_file_info(file_path):
    return {
        'name': file_path.name,
        'lines': count_lines(file_path),
        'size': file_path.stat().st_size if file_path.exists() else 0,
    }


def scan_project_structure():
    structure = {
        'backend': {'files': [], 'total_lines': 0},
        'frontend': {'files': [], 'total_lines': 0},
        'total_lines': 0,
    }

    # 后端：根目录下所有 .py 文件（排除 deploy.py 自身）
    for py_file in sorted(PROJECT_ROOT.glob("*.py")):
        if py_file.name == 'deploy.py':
            continue
        info = get_file_info(py_file)
        structure['backend']['files'].append({
            'path': py_file.name,
            **info,
        })
        structure['backend']['total_lines'] += info['lines']

    # 前端：static/ 下的 html/js/css
    if STATIC_DIR.exists():
        for ext in ('*.html', '*.js', '*.css'):
            for f in sorted(STATIC_DIR.rglob(ext)):
                rel_path = f.relative_to(PROJECT_ROOT)
                info = get_file_info(f)
                structure['frontend']['files'].append({
                    'path': str(rel_path),
                    **info,
                })
                structure['frontend']['total_lines'] += info['lines']

    structure['total_lines'] = (
        structure['backend']['total_lines']
        + structure['frontend']['total_lines']
    )
    return structure


def parse_api_routes():
    """解析 run.py 中的 Flask 路由"""
    apis = []
    if not APP_ENTRY.exists():
        return apis

    content = APP_ENTRY.read_text(encoding='utf-8')

    route_pattern = re.compile(
        r"@app\.route\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*methods\s*=\s*\[([^\]]+)\])?\s*\)"
    )
    func_pattern = re.compile(r"def\s+(\w+)\s*\([^)]*\):")

    for m in route_pattern.finditer(content):
        url = m.group(1)
        methods = m.group(2) if m.group(2) else "'GET'"
        methods = methods.replace("'", "").replace('"', "").replace(" ", "")

        func_match = func_pattern.search(content, m.end())
        if not func_match:
            continue
        func_name = func_match.group(1)

        doc_match = re.search(
            r'"""(.*?)"""', content[func_match.end():func_match.end() + 500], re.DOTALL
        )
        description = ""
        if doc_match:
            description = doc_match.group(1).strip().split('\n')[0].strip()

        apis.append({
            'url': url,
            'methods': methods,
            'function': func_name,
            'description': description,
        })

    return apis


def generate_code_md(structure, apis):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    content = f"""# YouTube 下载器 - 项目代码文档

> 自动生成时间: {now}

## 📋 项目概述

**项目名称**: YouTube 下载器
**项目简介**: 基于 yt-dlp + bgutil POT 的 YouTube 下载 Web 应用，支持单视频解析、下载队列、实时进度推送等功能。
**技术栈**: Python + Flask + JavaScript + SQLite + SSE + yt-dlp

---

## 📁 目录结构

```
youtube-down/
├── run.py                    # Flask 应用主入口
├── config.py                 # 配置常量（支持 .env）
├── models.py                 # 数据库模型（SQLite）
├── downloader.py             # 下载核心逻辑（yt-dlp 封装）
├── logger.py                 # 日志工具
├── pot_provider.py           # bgutil POT provider 管理
├── deploy.py                 # 上线预处理脚本
├── static/                   # 前端静态资源
│   ├── index.html
│   ├── app.js
│   └── style.css
├── scripts/                  # 辅助脚本 / 压测工具
├── pot-provider/             # bgutil POT provider（子模块）
├── downloads/                # 下载文件存放目录
├── log/                      # 运行日志
└── data.db                   # SQLite 数据库
```

---

## 📊 代码统计

### 后端文件详情

| 文件路径 | 代码行数 |
|---------|---------|
"""

    for f in sorted(structure['backend']['files'], key=lambda x: x['path']):
        content += f"| {f['path']} | {f['lines']:,} |\n"
    content += f"\n**后端代码总计**: {structure['backend']['total_lines']:,} 行\n\n"

    content += "### 前端文件详情\n\n| 文件路径 | 代码行数 |\n|---------|---------|\n"
    for f in sorted(structure['frontend']['files'], key=lambda x: x['path']):
        content += f"| {f['path']} | {f['lines']:,} |\n"
    content += f"\n**前端代码总计**: {structure['frontend']['total_lines']:,} 行\n\n"

    content += f"**项目总代码量**: {structure['total_lines']:,} 行\n\n"

    content += """---

## 🔌 后端 API 接口文档

| 接口地址 | 方法 | 处理函数 | 功能描述 |
|---------|------|---------|---------|
"""
    for api in apis:
        desc = api['description'] or '-'
        content += f"| `{api['url']}` | {api['methods']} | `{api['function']}` | {desc} |\n"

    content += f"\n**API 接口总数**: {len(apis)} 个\n\n"
    content += "---\n\n*本文档由 deploy.py 自动生成*\n"
    return content


def main():
    print_header("YouTube 下载器 - 上线预处理脚本")

    print("\n此脚本将执行以下操作:")
    print("  1. 删除 log/ 目录下的所有日志文件")
    print("  2. 删除本地数据库文件 data.db (含 WAL/SHM)")
    print("  3. 清理 downloads/ 目录下的所有文件")
    print("  4. 扫描项目结构并统计代码行数")
    print("  5. 解析所有 Flask 路由")
    print("  6. 生成/更新 CODE.md 文档")

    if not confirm("\n是否继续执行?"):
        print("\n已取消操作")
        sys.exit(0)

    print_step(1, 6, "清理日志文件")
    log_count = clean_logs()
    print(f"  共清理 {log_count} 个日志文件")

    print_step(2, 6, "清理数据库文件")
    db_count = clean_database()

    print_step(3, 6, "清理下载目录")
    temp_count = clean_downloads()

    print_step(4, 6, "扫描项目结构")
    structure = scan_project_structure()
    print(f"  后端文件: {len(structure['backend']['files'])} 个, {structure['backend']['total_lines']:,} 行")
    print(f"  前端文件: {len(structure['frontend']['files'])} 个, {structure['frontend']['total_lines']:,} 行")
    print(f"  总计: {structure['total_lines']:,} 行")

    print_step(5, 6, "解析 API 路由")
    apis = parse_api_routes()
    print(f"  共发现 {len(apis)} 个 API 接口")

    print_step(6, 6, "生成 CODE.md 文档")
    try:
        content = generate_code_md(structure, apis)
        CODE_MD_FILE.write_text(content, encoding='utf-8')
        print(f"  ✓ 文档已生成: {CODE_MD_FILE.name}")
        print(f"  文档大小: {len(content):,} 字符")
    except Exception as e:
        print(f"  ✗ 生成失败: {e}")
        sys.exit(1)

    print_header("预处理完成")
    print("\n执行结果:")
    print(f"  ✓ 清理日志文件: {log_count} 个")
    print(f"  ✓ 清理数据库文件: {db_count} 个")
    print(f"  ✓ 清理下载文件: {temp_count} 个")
    print(f"  ✓ 项目代码统计: {structure['total_lines']:,} 行")
    print(f"    - 后端: {structure['backend']['total_lines']:,} 行")
    print(f"    - 前端: {structure['frontend']['total_lines']:,} 行")
    print(f"  ✓ API 接口数量: {len(apis)} 个")
    print(f"  ✓ 文档已更新: {CODE_MD_FILE.name}")

    print("\n" + "=" * 60)
    print("项目已准备好发布到 GitHub!")
    print("=" * 60 + "\n")


if __name__ == '__main__':
    main()
