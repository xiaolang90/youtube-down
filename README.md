# YouTube 视频下载器

基于 yt-dlp + Flask 的 YouTube 下载 Web 应用,带 PO Token 生成、aria2c 并发加速和下载历史管理。

## 功能

- 解析视频元数据、选择清晰度/格式后下载
- SSE 实时进度推送,可取消、删除、清空
- 自动托管 `bgutil-ytdlp-pot-provider` Node 服务,解决 YouTube GVS 403
- 下载前刷新 integrityToken,绕过 googlevideo 单连接限速
- 可选 aria2c 多连接下载(实测音频从 ~40 KiB/s → ~5 MiB/s)
- 下载历史持久化到 SQLite,支持搜索、在 Finder 中显示
- `/api/health` 检测 yt-dlp / node / aria2c / pot-provider 状态

## 依赖

| 组件 | 版本/说明 |
|---|---|
| Python | 3.10+ |
| Node.js | ≥ 20(pot-provider 运行时) |
| yt-dlp | **nightly**,需支持 `--js-runtimes node`(`pip install --pre yt-dlp yt-dlp-ejs`) |
| aria2c | 可选但强烈推荐:`brew install aria2` / `apt install aria2` |
| Chrome | 用于读取登录 cookies(固定) |

## 快速开始

```bash
# 安装 Python 依赖
pip install --pre yt-dlp yt-dlp-ejs
pip install flask flask-cors

# 启动
python run.py
# 打开 http://localhost:8080
```

## 配置

复制 `.env.example` 为 `.env` 后按需修改:

| 变量 | 默认 | 说明 |
|---|---|---|
| `YT_DLP_PATH` | `yt-dlp` | yt-dlp 可执行路径,必须是 nightly |
| `HOST` / `PORT` | `0.0.0.0` / `8080` | Flask 监听地址 |
| `MAX_CONCURRENT` | `1` | 最大并发下载数 |
| `ARIA2C_PATH` | 自动 `which` | aria2c 路径,留空则回退 http-chunk-size |
| `ARIA2C_CONNECTIONS` | `16` | aria2c 并发连接数 |
| `HTTP_CHUNK_SIZE` | `100M` | 无 aria2c 时的分块大小 |

## 项目结构

```
run.py            Flask 入口,路由 + SSE + 任务调度
downloader.py     yt-dlp 封装,元数据解析和下载执行
pot_provider.py   bgutil pot-provider Node 子进程生命周期管理
models.py         SQLite 任务表,历史记录
config.py         .env 加载 + 运行时配置
logger.py         统一日志
pot-provider/     bgutil-ytdlp-pot-provider 源码(TypeScript,需构建)
static/           前端 SPA
scripts/          压测与诊断脚本
```

## 常见问题

- **下载 403**:pot-provider 未启动或未构建。检查 `/api/health`,或重新 `cd pot-provider && npm install && npx tsc`。
- **音频速度只有几十 KiB/s**:安装 aria2c;或确认 `ARIA2C_CONNECTIONS ≥ 8`。
- **yt-dlp 报 SABR / n-challenge 错**:yt-dlp 版本过旧,升级到 nightly。
