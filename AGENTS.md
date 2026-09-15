# AGENTS.md

本文件是 `Xray-core` fork 二次开发的仓库级协作规则，适用于仓库根目录及其全部子目录。若后续某个子目录提供更具体的 `AGENTS.md`，以更具体的规则为准。

## 项目定位

- 上游仓库：`https://github.com/XTLS/Xray-core`
- 当前 fork：`https://github.com/qiudesong/Xray-core`
- Go module：`github.com/xtls/xray-core`
- `origin` 指向当前 fork；自动同步 workflow 使用 `https://github.com/XTLS/Xray-core.git` 作为上游源。若本地另外配置 `upstream`，不得与 `origin` 混用。
- `main` 是上游镜像分支，只包含同步后的上游代码，以及维护当前 fork 所必需的 CI、发布和仓库规则配置；业务定制不得直接合入 `main`。
- `release` 是长期存在的 fork 定制集成和发布分支，包含已同步的 `main` 以及审核通过的 fork 功能，也是 `mod` tag 的唯一来源。
- `automation/sync-upstream` 是自动同步 workflow 使用的临时分支，不用于人工开发。

## 上游兼容原则

- fork 改动应尽量小、可审查、可回滚，减少后续同步 `XTLS/Xray-core` 时的冲突面。
- 能通用于上游的修复或增强，优先评估向上游提交；不要在 fork 中长期维护同一份通用补丁。
- 优先以新增配置、可选行为或局部适配实现功能，保持已有命令行参数、配置结构、默认值、API、协议行为和发布产物兼容。
- 修改 `proxy/`、`transport/`、`app/`、`features/`、`infra/conf/` 或 `core/` 时，必须检查调用链和配置映射，不能只验证单个包能够编译。
- 网络协议、加密、认证、路由和流量统计属于高风险兼容面；变更时应覆盖成功路径、失败路径、并发、超时、取消和资源释放。
- 新增或修改 metrics 时：
  - 优先新增明确、稳定的指标名，不复用旧名称表达不同语义。
  - 指标类型、单位、label 名称和语义一旦发布即视为兼容面。
  - 禁止使用用户 ID、邮箱、IP、域名、UUID、完整路由名等无界值作为 label，避免高基数和敏感信息泄露。
  - 热路径采集不得引入明显锁竞争、无界内存增长或阻塞网络处理。
- 不要直接复制其他项目的完整实现；先识别 Xray-core 已有抽象和注册方式，再做最小适配。

## 分支策略

- 长期分支职责：
  - `main`：跟随 `XTLS/Xray-core/main`，只接收上游同步和维护 fork 所必需的 CI、发布、文档及仓库规则变更。
  - `release`：集成和发布 fork 定制代码，通过 PR 接收 `main`、`feature/*` 和 `fix/*` 的变更。
- 业务功能从最新的 `release` 创建短期分支，并通过 PR 合并回 `release`：

  ```text
  feature/<short-description>
  fix/<short-description>
  chore/<short-description>
  ```

- 不直接在 `main` 或 `release` 上开发；每个独立功能或修复使用一个分支和一个 PR。
- 只影响上游同步、CI、发布或仓库规则且应落在 `main` 的变更，从最新 `main` 创建 `chore/<short-description>`，通过 PR 合并回 `main`，再通过 `main -> release` 同步 PR 传播。
- 开始业务开发前先确认工作区干净，并以 fast-forward 方式同步 `release`：

  ```bash
  rtk git status --short --branch
  rtk git switch release
  rtk git pull --ff-only origin release
  rtk git switch -c feature/<short-description>
  ```

- 不得自动 `reset`、`clean`、`stash`、覆盖用户改动或切换含未提交改动的分支；发现工作区不干净或基线异常时，先报告。
- 开发期间优先把最新 `release` merge 到功能分支。只有用户明确要求且分支尚未共享时才 rebase；不得强制推送 `main`、`release` 或来源不明的共享分支。
- `main -> release` 同步必须通过 PR 并使用 merge commit，保留 `main` 在 `release` 历史中的祖先关系；不能 squash 或 rebase 该同步 PR。遇到冲突时在 PR 分支中解决并重新通过 CI。
- commit 使用与变更性质匹配的 Conventional Commit type，例如 `feat:`、`fix:`、`perf:`、`refactor:`、`test:`、`docs:`、`build:`、`ci:` 或 `chore:`。`mod` 是 fork 发布 tag 的后缀，不是通用 commit type。
- 合并前检查完整 diff，排除无关格式化、意外生成文件、真实配置、密钥、日志和本地构建产物。
- 除非用户明确要求，不执行 commit、push、创建或合并 PR、删除分支、打 tag、创建 GitHub Release 或部署。workflow 已配置的自动动作不等于授权人工执行对应操作。

## 推荐开发流程

1. 明确需求范围、兼容面、配置变化、指标变化、性能影响和回滚方式。
2. 确认最新 `main` 已通过同步 PR 合入 `release`；工作区干净并同步 `release` 后，再创建单一职责的 `feature/*` 或 `fix/*` 分支。
3. 阅读相关入口、接口、实现、注册代码和现有测试后再修改；不要根据文件名或类型名猜测行为。
4. 采用最小改动实现功能，并为新增行为补充与风险相称的测试。
5. 运行受影响包的定向测试，再执行仓库级格式和测试检查。
6. 查看完整 diff 和新增文件，确认没有无关变化或敏感信息。
7. 创建以 `release` 为 base 的 PR，等待必要检查通过并完成代码审核。
8. PR 合并后删除已完成的短期分支；未完成或实验性功能不得提前合入 `release`。
9. 需要发布时，只能从已验证且已推送到 `origin/release` 的提交创建 `mod` tag。

## 上游同步

- `.github/workflows/sync-upstream.yml` 每日运行一次，也支持手动触发。
- workflow 从 `XTLS/Xray-core` 获取 `main`，创建或更新 `automation/sync-upstream -> main` PR，等待以下检查通过后启用自动 squash merge：
  - `check-assets`
  - `check-proto`
  - `check-format`
  - `test (ubuntu-latest)`
  - `test (macos-latest)`
  - `test (windows-latest)`
- 自动同步依赖仓库 secret `SYNC_TOKEN`，以及 GitHub Actions 创建 PR、运行检查和 auto-merge 所需的仓库权限。
- 同步 PR 有冲突或检查失败时，应在 PR 中定位原因并以最小改动修复；不得绕过 required checks 或分支保护。
- 不手工使用 `automation/sync-upstream` 开发，也不在已有同步 PR 之外创建同名分支。
- 上游同步 PR 合入 `main` 后，必须创建或更新 `main -> release` 同步 PR，并以 merge commit 合并。当前仓库尚未配置专用的 `main -> release` 自动同步 workflow，因此在增加该自动化前由人工发起和跟踪，且不得创建重复 PR。
- 如果 `release` 已包含最新 `main`，不创建空的或重复的同步 PR。
- 如需本地核对上游差异，可在明确配置了 `upstream` 后执行只读检查：

  ```bash
  rtk git fetch upstream main
  rtk git log --oneline --left-right main...upstream/main
  rtk git diff main...upstream/main
  ```

  未确认工作区、提交图和目标分支前，不执行本地 merge 或 push。

## 版本和 tag

- fork 新发布 tag **必须使用 `mod`，不得使用 `custom`**。
- tag 格式统一为：

  ```text
  v<upstream-version>-mod.<revision>
  ```

  示例：

  ```text
  v26.9.9-mod.1
  v26.9.9-mod.2
  ```

- `<upstream-version>` 表示当前 fork 基于的上游版本，`<revision>` 从 `1` 开始递增。同一上游版本有新修订时只能增加 revision。
- 不创建新的 `v*-custom.*` tag；文档、Release 标题、镜像 tag 和部署配置中也不得继续使用 `custom` 作为新版本后缀。
- fork 独有版本不得复用或覆盖上游的普通 `v*` tag，避免来源和产物混淆。
- tag 对应提交必须位于 `origin/release` 历史中，并且关联 PR 和 required checks 已通过；不得从 `main`、功能分支或临时同步分支发布 fork tag。
- 已发布 tag 不移动、不覆盖、不重复使用；修复必须创建递增的新 `mod` tag。
- 推送 tag 本身不足以完整发布二进制；当前 `.github/workflows/release.yml` 和 `.github/workflows/docker.yml` 的正式发布路径由 GitHub Release 事件驱动。创建或发布 Release 前必须再次核对 tag、目标提交、资产缓存和 workflow 行为。
- 发布记录至少说明：上游基线、fork 改动、配置或指标兼容性变化、支持平台、镜像 tag/digest 以及回滚版本。

## 构建、生成代码和资源

- Go 版本以 `go.mod` 为准，不在文档中固定复制可能过期的小版本号。
- 主程序入口是 `./main`。本地基础构建命令：

  ```bash
  rtk env CGO_ENABLED=0 go build -o xray -trimpath -buildvcs=false -ldflags="-s -w -buildid=" -v ./main
  ```

- `.proto` 是 protobuf 定义的源文件。修改 `.proto` 后必须使用仓库已有生成入口重新生成，并检查全部生成差异：

  ```bash
  rtk go generate ./core
  ```

- 不直接手改 `*.pb.go`、`*_grpc.pb.go` 等生成文件，除非上游流程明确要求；生成工具或生成版本变化应与功能变更分开评估。
- `resources/geoip.dat`、`resources/geosite.dat` 和 Wintun 文件属于构建/测试资源，不提交进 Git；CI 通过 `Scheduled assets update` workflow 写入 Actions cache。
- `Scheduled assets update` 必须存在于默认分支 `main` 后才能稳定手动运行和定时运行。需要刷新缓存时，在 Actions 页面选择该 workflow，使用 `main` 手动执行并等待 `geodat`、`wintun` 成功。
- `Tests and Checkings`、`Build and Release` 依赖上述缓存。出现 `geoip.dat`、`geosite.dat` 或 `wintun.dll` 不存在时，先检查缓存是否命中以及资源更新 workflow 是否成功，不要把缺失资源误判为业务测试失败。
- 不提交 `xray`、`xray.exe`、`build_assets/`、`resources/*.dat`、压缩包、coverage 文件或其他本地产物。

## 必须执行的检查

- 修改 Go 代码后至少执行：

  ```bash
  rtk go run ./infra/vformat/main.go -mode check -pwd ./
  rtk go test -timeout 1h -v ./...
  ```

- 格式检查失败时，使用仓库格式化入口处理，而不是只运行普通 `gofmt`：

  ```bash
  rtk go run ./infra/vformat/main.go -pwd ./
  ```

- 先运行受影响包的定向测试以快速反馈，但不得用定向测试替代仓库级测试。
- 测试依赖本地 Geodata 时，必须先准备与 CI 等价的 `resources/geoip.dat` 和 `resources/geosite.dat`；若无法准备，应明确报告哪些测试未执行及原因，不能声称全量测试通过。
- 修改 `.proto` 或生成文件后，还要确认所有非 gRPC `*.pb.go` 的生成器版本头与 `core/config.pb.go` 一致，对应 CI 检查为 `check-proto`。
- 修改平台相关代码时，至少验证相应的 `GOOS/GOARCH` 构建；涉及 Windows 7、Android、MIPS、ARM、RISC-V、LoongArch、PPC、S390X 或 BSD 时，应参考现有 release workflow 的环境和 flags，不能用宿主机单一构建代替。
- 网络和并发测试若出现偶发失败，应保留日志并定位根因；只有确认是已知 flaky test 后才重跑，不能用一次重跑成功掩盖可复现问题。
- 文档或纯 workflow 改动可按风险缩减本地 Go 检查，但必须验证 YAML、触发条件、权限、表达式、分支名和所依赖的 secret/cache，并说明未运行 Go 测试的理由。
- 完成前始终执行：

  ```bash
  rtk git status --short
  rtk git diff --check
  rtk git diff
  ```

## 代码和测试要求

- 遵循现有包边界、命名和错误处理风格；避免与当前任务无关的重构、依赖升级或全仓格式化。
- 新增配置字段时，必须同时检查 JSON/YAML/TOML 映射、默认值、校验、protobuf 定义、文档和向后兼容行为。
- 新增协议、transport、inbound、outbound、app 或 feature 时，应沿用现有注册机制，确认主发行版 `main/distro/all` 能包含所需实现。
- goroutine 必须有明确退出条件；阻塞操作必须响应 context、超时或连接关闭；错误路径必须释放连接、buffer、timer 和其他资源。
- 测试应确定、可重复且不依赖公网、固定端口、真实时间长等待或执行顺序；端口测试优先使用动态分配。
- 修复缺陷时优先添加能在修复前失败、修复后通过的回归测试。不要为了通过测试删除断言、扩大超时或静默吞掉错误。
- 安全漏洞和协议可识别性问题不得公开写入 issue、普通日志或提交说明；按 `SECURITY.md` 的私密报告渠道处理。

## 禁止提交

- 密钥、token、cookie、证书私钥、真实服务器地址、用户数据或包含敏感信息的日志。
- 二进制、Actions 下载产物、缓存内容、coverage 输出、临时目录和 IDE 状态。
- 未经说明的依赖升级、生成器升级、全仓库格式化或与当前功能无关的重构。
- 用于临时调试的弱安全配置、跳过校验逻辑、永久 debug 日志或无界指标 label。
