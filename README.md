# HugBucket

S3-compatible gateway for Hugging Face Storage Buckets.

## Quick Start

### Docker Compose（推荐）

```bash
docker compose up -d
```

服务启动后：
- **S3 Gateway**: `http://localhost:9000`
- **Admin 管理面板**: `http://localhost:9001`

### Docker

```bash
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  -e AWS_ACCESS_KEY_ID=hugbucket \
  -e AWS_SECRET_ACCESS_KEY=hugbucket \
  -v hugbucket_data:/data \
  ghcr.io/exynos967/hugbucket
```

## 多 Token 负载均衡

HugBucket 支持配置多个 HF Token 实现负载均衡：

1. 启动后打开 **Admin 管理面板** `http://localhost:9001`
2. 在「Token 管理」页面添加多个 HF Token
3. 系统自动采用 **Round Robin** 策略轮询分发请求

Token 配置持久化存储在 `tokens.json` 中，无需重启服务。

### 单 Token 模式（回退）

如果 `tokens.json` 中没有配置任何 Token，系统会自动回退使用环境变量 `HF_TOKEN`。

## Usage

#### AWS CLI

```bash
aws --endpoint-url http://localhost:9000 s3 ls
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/file.txt
```

## 环境变量

| Variable | Description |
| --- | --- |
| `HF_TOKEN` | 单 Token 回退模式（推荐通过 Admin 面板配置多 Token） |
| `AWS_ACCESS_KEY_ID` | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key |
| `HUGBUCKET_TOKENS_FILE` | Token 配置文件路径（默认 `./tokens.json`） |

## Admin API

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/api/status` | 系统状态总览 |
| `GET` | `/api/tokens` | Token 列表 |
| `POST` | `/api/tokens` | 添加 Token |
| `DELETE` | `/api/tokens/{index}` | 删除 Token |
| `POST` | `/api/tokens/{index}/resolve` | 重新解析命名空间 |
| `GET` | `/api/buckets` | 所有存储桶用量 |
| `GET` | `/api/buckets/{ns}/{name}` | 存储桶详情 |

## Development

本项目使用 [uv](https://docs.astral.sh/uv/) 管理依赖。

```bash
uv sync

# 开发模式启动
HUGBUCKET_TOKENS_FILE=./tokens.json uv run hugbucket
```
