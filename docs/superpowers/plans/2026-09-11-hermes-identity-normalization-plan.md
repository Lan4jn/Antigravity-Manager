# Hermes 身份归一化：上游同步后的开发与部署计划

> 面向执行者：使用 executing-plans 或 subagent-driven-development 逐项执行。复选框表示真实完成状态，历史测试不能替代合并后的验证。

**更新日期：** 2026-09-12
**目标：** 在官方 4.7.1 基线上验证精准身份归一化，交付前后端统一镜像，完成 Chat/Responses 生产 429 闭环。
**架构：** 复用共享 mapper 的 Cow/strip_prefix helper，在 L1 cache lookup 前替换；保留上游 requestType、会话、限流机制，通过实际协议证据定位剩余问题。
**技术栈：** Rust stdlib、serde_json、现有 Cargo 测试、Axum、Vite、Docker BuildKit/Compose v2。

## 0. 当前状态

- [x] 官方 upstream/main 已同步到 734e2bde5d60cd7ecffe2467b5cab9cb154a3f5e，当前合并提交 1ae1fa5。
- [x] 本地修改已备份并恢复，无合并冲突；本地 main 与远端未被改写。
- [x] 已有身份 helper、缓存前接入、mapper 合成测试；Docker 名称及 Cargo 路径修正已保留。
- [x] 本次重写报告、Spec 和计划，区分历史证据与待验收项目。
- [ ] 对合并后的 4.7.1 执行完整相关验证。
- [ ] 重建并部署新的前后端统一镜像。
- [ ] 复查用户更新后仍出现的生产 429。

旧 dist/docker 与 dist 来自 4.6.9，可用于追溯，不能当作此次 4.7.1 成品。

| 文件 | 职责 |
|---|---|
| src-tauri/src/proxy/mappers/openai/request.rs | 唯一身份归一化、内容及 prefix hash 回归 |
| src-tauri/src/proxy/handlers/openai.rs、proxy/server.rs | 实际入口和协议转换测试，不复制身份 helper |
| proxy/rate_limit.rs、handlers/common.rs、token_manager.rs、common/session.rs | 核查上游行为，复现证实后才修改额外问题；路径均在 src-tauri/src 下 |
| docker/Dockerfile 及 backend/localdist 变体 | 统一镜像、正确二进制名与 Cargo 缓存路径 |
| docker/HERMES_UPDATE.md | 补入验证后的新镜像、更新及回滚信息 |
| dist/docker/ | 镜像包、校验值、构建信息和日志；不作为源码提交 |

## 1. 核对合并结果

- [ ] 阅读 734e2bd、a620ec2 与 mapper/handler 差异，确认身份归一化先于 SI key 和 L1 lookup。
- [ ] 核对四种 instruction 形式及两个 handler 到共享 mapper 的调用链。
- [ ] 确认动态 requestType、derive_session_scoped/bump、quota 分类未被本地补丁覆盖。
- [ ] 审查并提交本地业务修改，记录可复现源码 SHA；确认恢复完整后再处理备份 stash。

运行：

    git merge-base --is-ancestor upstream/main HEAD
    git diff --check
    git diff -- src-tauri/src/proxy/mappers/openai/request.rs docker/

## 2. 补齐回归

- [ ] 运行现有 Hermes 测试，复用现有 helper，不重复实施已有功能。
- [ ] 补齐 BOM、大小写、缺句号、前导空白、后缀再次引用；断言严格等式与 Cow::Borrowed。
- [ ] 补齐 tool 返回、媒体、tool_choice/thinking、多轮历史，与“手动只替换前缀”的 mapper 输出比较。
- [ ] 保留 20 个合成工具、四种 instruction 形式、实际 prefix hash 及重复转换测试。
- [ ] 增加实际 handler/mock 上游覆盖：Chat、/v1/responses、/responses，stream true/false，工具往返。直接构造 OpenAIRequest.instructions 不等于 HTTP Responses 已验证。
- [ ] 对新增测试确认能捕获对应回归，再做必要的最小修复。

在具备 Linux 构建依赖的环境、src-tauri 目录执行：

    cargo test --release --lib hermes -- --test-threads=1
    cargo test --release --lib unrelated_identity_is_borrowed_unchanged
    cargo test --release --lib proxy::mappers::openai -- --test-threads=1
    cargo test --release --lib proxy::handlers::openai -- --test-threads=1
    cargo test --release --lib proxy::handlers::common -- --test-threads=1
    cargo test --release --lib proxy::rate_limit -- --test-threads=1
    cargo test --release --lib proxy::token_manager -- --test-threads=1
    cargo test --release --lib proxy::common::session -- --test-threads=1
    cargo test --release --lib proxy::session_manager -- --test-threads=1
    cargo check --lib
    cargo fmt --check

记录失败属于上游基线还是本地修改。全仓格式化已有历史差异时限定修改范围并说明，不顺手格式化全部文件。Windows 缺 libclang 不能作为测试通过。

## 3. 构建统一镜像

- [ ] 用同一份已审查源码构建前后端，避免复用旧 dist。
- [ ] 确认 cargo build --release --bin antigravity-tools 与 /usr/local/cargo 镜像源、缓存配置。
- [ ] 使用新唯一标签，记录源码 SHA、工作区状态、平台和构建命令。

在仓库根目录、Linux amd64 / BuildKit 环境执行：

    set -eu
    TAG="antigravity-manager:4.7.1-hermes-$(git rev-parse --short HEAD)-$(date +%Y%m%d%H%M%S)"
    docker build --platform linux/amd64 -f docker/Dockerfile -t "$TAG" .

需加速时使用已验证镜像源及 --build-arg USE_MIRROR=true。若使用 localdist，必须先 npm ci --legacy-peer-deps、npm run build，再用 docker/Dockerfile.backend.localdist 构建。最终仍包含前端与后端。

- [ ] 临时数据/密钥的隔离容器中验证 health=ok、HTML/JS 字节一致、二进制可执行。
- [ ] 导出镜像与校验值，构建信息随包交付。

    set -o pipefail
    ARCHIVE="antigravity-manager-4.7.1-hermes-linux-amd64-$(date +%Y%m%d%H%M%S).tar.gz"
    docker save "$TAG" | gzip -1 > "$ARCHIVE"
    sha256sum "$ARCHIVE" > "$ARCHIVE.sha256"
    docker image inspect "$TAG" --format '{{.Id}} {{.Os}}/{{.Architecture}}'

## 4. 更新已有容器与回滚

以下 Bash 示例适用于仓库默认 Compose 布局。自定义部署必须沿用原 -p、-f、--env-file、容器名和数据挂载源；先确认实际值再执行。沿用原密钥，不把示例 API_KEY=test 带入生产。

- [ ] 核对原容器、image ID、端口、账号数据及有无外挂 /app/dist；外挂前端会遮住镜像内新产物。
- [ ] 镜像包在目标机执行 sha256sum -c "$ARCHIVE.sha256"、docker load -i "$ARCHIVE"；将 TAG 设置为记录的新镜像标签。
- [ ] 保存旧镜像、原 Compose/.env 与一致数据备份，限制备份访问权限。

默认部署示例，先构建/导入新镜像，之后才短暂停机：

    set -eu
    SERVICE=antigravity-manager
    ROLLBACK="antigravity-manager:rollback-$(date +%Y%m%d%H%M%S)"
    docker tag "$(docker inspect --format '{{.Image}}' "$SERVICE")" "$ROLLBACK"
    OVERRIDE=docker/compose.hermes-image.yml
    printf 'services:\n  antigravity-manager:\n    image: %s\n' "$TAG" > "$OVERRIDE"
    docker compose -f docker/docker-compose.yml stop "$SERVICE"
    BACKUP="$HOME/antigravity-data-$(date +%Y%m%d%H%M%S).tar.gz"
    if ! (umask 077; tar -czf "$BACKUP" -C "$HOME" .antigravity_tools); then
      docker compose -f docker/docker-compose.yml start "$SERVICE"
      exit 1
    fi
    docker compose -f docker/docker-compose.yml -f "$OVERRIDE" up -d --no-build --pull never "$SERVICE"
    docker inspect --format '{{.Image}}' "$SERVICE"
    docker image inspect "$TAG" --format '{{.Id}}'
    curl --fail --retry 12 --retry-connrefused --retry-delay 5 http://127.0.0.1:8045/health

- [ ] 两个镜像 ID 一致，health 为本次 4.7.1，页面/资源成功；访问地址按实际绑定及反代调整。
- [ ] 再做第 5 节真实推理验收，health 正常不能替代生产请求。
- [ ] 失败则切回记录的 ROLLBACK 标签，用相同部署参数重建容器：

    printf 'services:\n  antigravity-manager:\n    image: %s\n' "$ROLLBACK" > "$OVERRIDE"
    docker compose -f docker/docker-compose.yml -f "$OVERRIDE" up -d --no-build --pull never "$SERVICE"
    curl --fail --retry 12 --retry-connrefused --retry-delay 5 http://127.0.0.1:8045/health

不要删除数据卷。跨版本配置回滚需验证；确需恢复备份时先停服务并评估备份后的数据变更。部署记录保存具体镜像标签、归档和备份路径，不能只依赖 shell 变量。

## 5. 生产 429 复查

- [ ] 确认用户实际报错的实例和镜像 ID，检查脱敏请求的身份命中情况。
- [ ] 刷新配额，记录时间、模型映射、匿名账号、Retry-After、details.reason 和 quotaResetDelay；面板百分比与通用错误都不能单独定因。
- [ ] 同协议、模型、tools/history/参数下只改身份：中性、规范 Hermes、手动 Antigravity。按无工具/有工具分组，避免 requestType 混淆。
- [ ] 先非流式再流式，覆盖 Chat 与 Responses（包括别名）；记录请求摘要、HTTP 状态、结束事件及工具结果。
- [ ] 冷却期间等待或使用隔离实例，不反复清空生产限流，不以无限重试取得一次成功。
- [ ] 原 Hermes custom provider 完整请求保留全部工具，完成一次安全工具调用及第二轮上下文；随后由用户完成 QQBot 验收。
- [ ] mock 配额/容量错误，确认仍正确传播。

中性和 Antigravity 也失败时查容量、配额、项目或会话；仅 Hermes 失败时查命中与其他指令残留。证据不足时记录待补项，不扩大匹配规则碰运气。

## 6. 完成标准

合并后回归、统一镜像、部署验证、生产矩阵均有证据；Chat/Responses 文本、工具和上下文正常；真实限流仍正确失败。随后更新 [报告](../reviews/2026-09-11-hermes-identity-investigation.md) 和 [Spec](../specs/2026-09-11-hermes-identity-normalization-spec.md) 状态。

本次仅同步源码和重写文档，不代表上述待办已经执行。旧 4.6.9 镜像、8 项测试和容器验证只作历史记录。
