# Hermes 身份修复：Docker 更新

此补丁仅在共享 OpenAI mapper 中、缓存 key 计算前替换完整 Hermes 官方身份前缀。Chat Completions、`/v1/responses` 和 `/responses` 共用该转换层。既有缓存清洗和系统提示分段规则继续生效；“逐字保留后缀”指身份归一化函数本身，不代表取消原有清洗。

## 更新已有 Linux Docker 服务

### 使用已编译的前后端统一镜像

若已取得 `antigravity-manager-hermes-20260912-linux-amd64.tar.gz`，在目标 Linux x86_64 主机导入即可，无需安装 Node/Rust 或重新编译：

```bash
sha256sum -c antigravity-manager-hermes-20260912-linux-amd64.tar.gz.sha256
docker load -i antigravity-manager-hermes-20260912-linux-amd64.tar.gz
```

此镜像包含本次编译的 Web 前端 `/app/dist` 和 Linux 后端 `/app/antigravity-tools`，同一容器提供页面和 API。保留旧镜像及数据备份后，将原 Compose 服务的 `image` 改为 `antigravity-manager:hermes-20260912`，使用原部署的 Compose 参数执行：

```bash
docker compose up -d --no-build --pull never antigravity-manager
curl --fail http://127.0.0.1:8045/health
```

沿用原端口、环境变量和数据卷；若挂载了外部 `/app/dist`，该挂载会遮住镜像内新前端，更新时需使用镜像内前端。完整备份与回滚步骤见下文。

### 从源码构建

在包含本次修改的仓库根目录执行。需要 Docker Engine 和 Compose v2。单纯拉取官方 `latest` 不会包含尚未发布的本地补丁。下面假设现有容器名为 `antigravity-manager`，由 `docker/docker-compose.yml` 管理；如部署时使用了其他 Compose 文件、项目名、端口或挂载路径，沿用原参数，不能直接切换到示例配置。

先保留旧镜像，然后构建新镜像，构建期间旧服务继续运行：

```bash
set -eu
OLD_IMAGE=$(docker inspect --format '{{.Image}}' antigravity-manager)
ROLLBACK="antigravity-manager:rollback-$(date +%Y%m%d-%H%M%S)"
docker tag "$OLD_IMAGE" "$ROLLBACK"
TAG="antigravity-manager:hermes-$(git rev-parse --short HEAD)-$(date +%Y%m%d-%H%M%S)"
docker build -f docker/Dockerfile -t "$TAG" .
printf 'new=%s\nrollback=%s\n' "$TAG" "$ROLLBACK"
```

若只更新后端，可用下列命令**替代**上面的完整构建。旧镜像必须包含 `/app/dist`，且其前端与当前后端兼容：

```bash
docker build -f docker/Dockerfile.backend \
  --build-arg FRONTEND_IMAGE="$ROLLBACK" -t "$TAG" .
```

创建镜像覆盖文件（以后更新与回滚都带上这个文件）：

```bash
cat > docker/compose.hermes-image.yml <<EOF
services:
  antigravity-manager:
    image: $TAG
EOF
```

保留原 `.env`、API key、网络设置和数据挂载。仓库 Compose 默认使用 host 网络、绑定本机地址，并有 `API_KEY=test` 默认值；必须沿用实际部署的密钥与访问设置。不要打印容器的完整环境变量。

为一致备份短暂停机；以下数据目录与仓库默认 Compose 一致，自定义部署应替换为实际挂载源：

```bash
docker compose -f docker/docker-compose.yml stop antigravity-manager
BACKUP="$HOME/antigravity-data-$(date +%Y%m%d-%H%M%S).tar.gz"
if ! (umask 077; tar -czf "$BACKUP" -C "$HOME" .antigravity_tools); then
  docker compose -f docker/docker-compose.yml start antigravity-manager
  exit 1
fi
docker compose -f docker/docker-compose.yml -f docker/compose.hermes-image.yml \
  up -d --no-build --pull never antigravity-manager
docker inspect --format '{{.Image}}' antigravity-manager
docker image inspect --format '{{.Id}}' "$TAG"
curl --fail --retry 12 --retry-connrefused --retry-delay 5 http://127.0.0.1:8045/health
```

两个镜像 ID 应一致。此补丁未修改版本号，`/health` 仍可能显示 `4.6.9`，不能仅靠版本号判断更新成功。备份包含账号凭据，应限制访问。不要删除数据卷。

## 请求验收

在本机设置 `MANAGER_API_KEY` 为现有密钥，不把真实密钥写入文档或版本库。以下是合成请求，先验 Chat，再验 Responses：

```bash
curl --fail-with-body http://127.0.0.1:8045/v1/chat/completions \
  -H "Authorization: Bearer $MANAGER_API_KEY" -H 'Content-Type: application/json' \
  -d '{"model":"gemini-3.8-flash-high","stream":false,"messages":[{"role":"system","content":"You are Hermes Agent, an intelligent AI assistant created by Nous Research.\n\nReply with exactly HERMES_OK."},{"role":"user","content":"hello"}]}'
curl --fail-with-body http://127.0.0.1:8045/v1/responses \
  -H "Authorization: Bearer $MANAGER_API_KEY" -H 'Content-Type: application/json' \
  -d '{"model":"gemini-3.8-flash-high","stream":false,"instructions":"You are Hermes Agent, an intelligent AI assistant created by Nous Research.\n\nReply with exactly HERMES_OK.","input":"hello"}'
```

再以 `stream:true` 重试两种请求，并用原 Hermes provider 验证完整提示、工具调用及连续两轮上下文。最小请求成功不能替代完整 Hermes / QQBot 验收；真实配额不足仍可返回 429/503。中间引用 Hermes 身份应保持不变，其上游 HTTP 状态并不由此补丁保证。

## 回滚

用先前记录的 `ROLLBACK` 标签替换覆盖文件的镜像，再重建容器：

```bash
cat > docker/compose.hermes-image.yml <<EOF
services:
  antigravity-manager:
    image: $ROLLBACK
EOF
docker compose -f docker/docker-compose.yml -f docker/compose.hermes-image.yml \
  up -d --no-build --pull never antigravity-manager
curl --fail --retry 12 --retry-connrefused --retry-delay 5 http://127.0.0.1:8045/health
```

本补丁无数据迁移，通常仅回滚镜像即可；恢复数据备份需先停服务，并评估备份之后的数据变更。
