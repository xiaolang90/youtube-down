# YouTube 下载器 - 项目代码文档

> 自动生成时间: 2026-04-13 06:21:33

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
| config.py | 50 |
| downloader.py | 614 |
| logger.py | 81 |
| models.py | 116 |
| pot_provider.py | 151 |
| run.py | 389 |

**后端代码总计**: 1,401 行

### 前端文件详情

| 文件路径 | 代码行数 |
|---------|---------|
| static/app.js | 557 |
| static/index.html | 165 |
| static/style.css | 94 |

**前端代码总计**: 816 行

**项目总代码量**: 2,217 行

---

## 🔌 后端 API 接口文档

| 接口地址 | 方法 | 处理函数 | 功能描述 |
|---------|------|---------|---------|
| `/` | GET | `index` | - |
| `/api/parse` | POST | `parse_video` | - |
| `/api/download` | POST | `start_download` | - |
| `/api/download/<task_id>/progress` | GET | `stream_progress` | - |
| `/api/download/<task_id>/cancel` | POST | `cancel_download` | - |
| `/api/download/<task_id>` | DELETE | `remove_download` | - |
| `/api/downloads/all` | DELETE | `clear_all_downloads` | Cancel all active tasks, delete all DB records, and remove downloaded files. |
| `/api/history` | GET | `history` | - |
| `/api/file/<task_id>` | GET | `serve_file` | - |
| `/api/file/<task_id>/reveal` | POST | `reveal_file` | Reveal the downloaded file in the OS file manager (Finder on macOS). |
| `/api/active` | GET | `active_downloads` | Return list of currently active/recent tasks for the download queue display. |
| `/api/settings` | GET | `get_settings` | Return status of yt-dlp, node.js, and the POT provider server. |
| `/api/health` | GET | `health_check` | Return status of yt-dlp, node.js, and the POT provider server. |
| `/api/settings` | PUT | `update_settings` | - |

**API 接口总数**: 14 个

---

*本文档由 deploy.py 自动生成*
