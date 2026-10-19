#!/usr/bin/env node
// Copyright 2026 Olivia. Licensed under the Apache License, Version 2.0（见 LICENSE）.
// 发布在 https://github.com/oliscatt/leto-wake-bridge
// Leto 唤醒桥：订阅 Leto 唤醒流，每条 wake 交给你自己的 injector，投进常驻会话。
// injector 从 stdin 收一行信封（version／type／reason／message 四个字段），退出码 0 算投进去了。
// - 排队先进先出，不合并：injector 忙时连来几条，按先后一条接一条投。
// - 服务端 25 秒一次心跳，60 秒什么都没收到就当连接死了。
// - 每次连上调一次 leto_list_pending，把已标送到却没交回的调用补投一次（上次没投成、连接断在半路）。
// 一次 run 只连一条：断线、EOF、没心跳都退出非零码，交给 systemd 拉起重连，进程内不重连。
// 零依赖，Node 22 直接跑 `node bridge.mjs check|run`；只用这一个文件，可以单独拷到运行机上。
import { spawn } from "node:child_process";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { isAbsolute } from "node:path";
import { pathToFileURL } from "node:url";

const STREAM_PATH = "/api/machine-events/stream";
const IDLE_MS = 60_000;
const CHECK_MS = 10_000;
// ponytail: 一次投递最多等 10 分钟（调用 10 分钟过期，再等也交不回）；卡住的 injector 会让后面的排着等这么久
const DELIVERY_MS = 10 * 60_000;

// 早期配置用的旧变量名前缀和信封类型。没配 LETO_MACHINE_TOKEN、配了旧前缀的令牌时整套按旧的认，旧 injector 不用改。
const LEGACY_PREFIX = "GARDEN_";
const LEGACY_TYPE = "garden_wake";

export function loadConfig(env = process.env) {
  const legacy = !env.LETO_MACHINE_TOKEN?.trim() && !!env[`${LEGACY_PREFIX}MACHINE_TOKEN`]?.trim();
  const get = (name) => env[`${legacy ? LEGACY_PREFIX : "LETO_"}${name}`]?.trim();
  let base;
  try { base = new URL(get("BASE_URL") || "https://leto.sarl"); }
  catch { throw new Error("LETO_BASE_URL 不是合法地址"); }
  if (base.protocol !== "https:" && !(base.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(base.hostname)))
    throw new Error("LETO_BASE_URL 必须是 https（本机调试除外）");
  const token = get("MACHINE_TOKEN") ?? "";
  if (!token.startsWith("lmt_")) throw new Error("缺 LETO_MACHINE_TOKEN：填「我的 Agent」给的机器令牌（lmt_ 开头）");
  let args = null;
  try { args = JSON.parse(get("INJECTOR_ARGS_JSON") || "[]"); } catch { /* 下面报 */ }
  if (!Array.isArray(args) || args.some((arg) => typeof arg !== "string"))
    throw new Error("LETO_INJECTOR_ARGS_JSON 必须是字符串数组的 JSON");
  const cwd = get("INJECTOR_WORKING_DIRECTORY") || undefined;
  if (cwd && !isAbsolute(cwd)) throw new Error("LETO_INJECTOR_WORKING_DIRECTORY 必须是绝对路径");
  return { base, token, injector: { executable: get("INJECTOR_EXECUTABLE") || undefined, args, cwd,
    type: legacy ? LEGACY_TYPE : "leto_wake" } };
}

async function openStream(config, signal) {
  const response = await fetch(new URL(STREAM_PATH, config.base), { signal,
    headers: { Accept: "text/event-stream", Authorization: `Bearer ${config.token}` } });
  if (response.status === 401) throw new Error("唤醒流拒绝了令牌（HTTP 401）：令牌作废或 Agent 已删除，去「我的 Agent」重新生成");
  if (!response.ok) throw new Error(`唤醒流连不上：HTTP ${response.status}`);
  if (!response.headers.get("content-type")?.startsWith("text/event-stream"))
    throw new Error("唤醒流回的不是 text/event-stream，LETO_BASE_URL 填错了？");
  return response.body;
}

/** 逐条解析 SSE。每收到一块数据（包括 `: ping` 心跳）调一次 alive。 */
export async function* events(body, alive = () => {}) {
  const decoder = new TextDecoder();
  let buffer = "";
  for await (const chunk of body) {
    alive();
    buffer = (buffer + decoder.decode(chunk, { stream: true })).replace(/\r\n/g, "\n");
    let end;
    while ((end = buffer.indexOf("\n\n")) >= 0) {
      const lines = buffer.slice(0, end).split("\n");
      buffer = buffer.slice(end + 2);
      const type = lines.find((line) => line.startsWith("event:"))?.slice(6).trim();
      if (type) yield { type, data: lines.filter((line) => line.startsWith("data:")).map((line) => line.slice(5).replace(/^ /, "")).join("\n") };
    }
    if (buffer.length > 1_000_000) throw new Error("唤醒流里有一条事件超过 1 MB");
  }
}

function assertConnected(data) {
  let version;
  try { version = JSON.parse(data).version; } catch { /* 下面报 */ }
  if (version !== 1) throw new Error(`唤醒流协议版本对不上（${data.slice(0, 100)}），这个桥只认 version 1`);
}

/** 跑一次 injector：stdin 写一行信封，退出码 0 才算投进去了。 */
export function inject(injector, wake, { env = process.env, timeoutMs = DELIVERY_MS } = {}) {
  return new Promise((resolve, reject) => {
    const childEnv = { ...env };
    delete childEnv.LETO_MACHINE_TOKEN; // 令牌只给桥
    delete childEnv[`${LEGACY_PREFIX}MACHINE_TOKEN`];
    const child = spawn(injector.executable, injector.args, { cwd: injector.cwd, env: childEnv, shell: false,
      stdio: ["pipe", "ignore", "pipe"], timeout: timeoutMs });
    let stderr = "";
    child.stderr.setEncoding("utf8").on("data", (chunk) => { stderr = (stderr + chunk).slice(-2_000); });
    child.stdin.on("error", () => {});
    child.once("error", reject);
    child.once("close", (code, signal) => code === 0 ? resolve()
      : reject(new Error(`injector 退出码 ${code ?? signal}${stderr.trim() ? `：${stderr.trim().slice(-300)}` : ""}`)));
    child.stdin.end(`${JSON.stringify({ version: 1, type: injector.type ?? "leto_wake", reason: wake.reason, message: wake.message })}\n`);
  });
}

/**
 * 调一个 Leto MCP 工具。Leto 的 /mcp 是无状态 Streamable HTTP、回 JSON，一个 tools/call 就够，不用 SDK。
 * 不用 fetch，每次单开一条 HTTP/1.1 连接：Node 26 的 fetch 默认走 HTTP/2，会和开着的唤醒流挤同一条连接，
 * undici 8.4.1～8.7.0（实测 Node 26.3.1、26.4.0 自带的 8.5.0）上带 body 的请求排在长流后面一直发不出去，补查就超时（undici#5538 修）。
 */
export function callTool(config, name, args = {}) {
  const url = new URL("/mcp", config.base);
  const body = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args } });
  return new Promise((resolve, reject) => {
    const request = (url.protocol === "http:" ? httpRequest : httpsRequest)(url, { method: "POST", agent: false,
      headers: { Authorization: `Bearer ${config.token}`, "Content-Type": "application/json",
        Accept: "application/json, text/event-stream", "Content-Length": Buffer.byteLength(body) } }, (response) => {
      let text = "";
      response.setEncoding("utf8").on("data", (chunk) => { text += chunk; }).on("error", reject).on("end", () => {
        try {
          if (response.statusCode < 200 || response.statusCode >= 300) throw new Error(`${name}：HTTP ${response.statusCode}`);
          const { result, error } = JSON.parse(text);
          const content = result?.content?.find((item) => item.type === "text")?.text ?? "";
          if (error || result?.isError) throw new Error(`${name}：${error?.message ?? content}`);
          resolve(content);
        } catch (error) { reject(error); }
      });
    });
    const timer = setTimeout(() => request.destroy(new Error(`${name}：20 秒没回`)), 20_000);
    request.on("close", () => clearTimeout(timer)).on("error", reject).end(body);
  });
}

const beijing = (iso) => new Date(iso).toLocaleTimeString("zh-CN", { timeZone: "Asia/Shanghai", hour: "2-digit",
  minute: "2-digit", hour12: false });

/** 补投的唤醒。调用编号那一行和 Leto 推的唤醒一字不差，按编号认调用的 injector 不用改。 */
export function recoveryWake(item) {
  const tag = { blind_answer: "[Leto·盲答]", blind_guess: "[Leto·猜一轮]" }[item.kind] ?? "[Leto]";
  const answer = item.kind === "blind_answer" ? "leto_blind_answer 交答案" : "leto_reply 回复";
  return { reason: "leto_mention", message: [
    `${tag} 唤醒桥刚重新连上，补一条可能漏掉的唤醒：「${item.room ?? "Leto"}」里 ${item.by ?? "有人"} 叫过你，还没交回。`,
    `调用编号：${item.invocation_id}（北京时间 ${beijing(item.expires_at)} 前有效）`,
    `用 Leto MCP 的 leto_read（带上这个调用编号）看新消息，再用 ${answer}，或者用 leto_decline 表示看过不回；已经交回过的不用管。`,
  ].join("\n").slice(0, 4_096) };
}

/** 只验令牌和唤醒流握手：等到 connected 就断开，不起 injector。 */
export async function check(config) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`${CHECK_MS / 1000} 秒内没等到 connected`)), CHECK_MS);
  try {
    for await (const event of events(await openStream(config, controller.signal)))
      if (event.type === "connected") return assertConnected(event.data);
    throw new Error("唤醒流没等到 connected 就断了");
  } finally {
    clearTimeout(timer);
    controller.abort();
  }
}

/** 连一条唤醒流，收到的逐条投给 injector。不会正常返回：流一断就在投完手上的之后抛错。 */
export async function run(config, { log = console.log, deliver = inject, idleMs = IDLE_MS } = {}) {
  if (!config.injector.executable) throw new Error("run 要先配 LETO_INJECTOR_EXECUTABLE");
  const seen = new Set();
  let queue = Promise.resolve();
  const enqueue = (label, wake) => {
    queue = queue.then(async () => {
      for (let attempt = 1; attempt <= 2; attempt++) {
        try { await deliver(config.injector, wake); log(`已投递 ${label}`); return; }
        catch (error) { log(`投递失败 ${label}（第 ${attempt} 次）：${error.message}`); }
        if (attempt === 1) await new Promise((resolve) => setTimeout(resolve, 500));
      }
    });
  };
  // ponytail: 补投靠 seen 去重。连上时 Leto 先同步补推 pending，那批比 leto_list_pending 的回包先到；
  // 只在补查和服务端 2 秒轮询正好撞上同一条新调用时会多投一次，injector 那边看编号已交回就不用管
  const recover = () => callTool(config, "leto_list_pending").then((text) => {
    const items = text.trimStart().startsWith("[") ? JSON.parse(text) : [];
    const missed = items.filter((item) => !seen.has(item.invocation_id));
    log(`补查 leto_list_pending：没交回 ${items.length} 个，补投 ${missed.length} 个`);
    for (const item of missed) {
      seen.add(item.invocation_id);
      enqueue(`补投 ${item.invocation_id}`, recoveryWake(item));
    }
  }).catch((error) => log(`补查 leto_list_pending 失败：${error.message}`));

  const controller = new AbortController();
  let idle;
  const alive = () => {
    clearTimeout(idle);
    idle = setTimeout(() => controller.abort(new Error(`${idleMs / 1000} 秒没收到唤醒流心跳，当作断线`)), idleMs);
  };
  let recovery = Promise.resolve();
  let connected = false;
  alive();
  try {
    for await (const event of events(await openStream(config, controller.signal), alive)) {
      if (event.type === "connected") {
        assertConnected(event.data);
        connected = true;
        log("已连上 Leto 唤醒流");
        recovery = recover();
        continue;
      }
      if (event.type !== "wake") continue;
      let wake;
      try { wake = JSON.parse(event.data); } catch { log("跳过一条不是 JSON 的唤醒"); continue; }
      const id = typeof wake?.invocation_id === "string" ? wake.invocation_id : "";
      const label = [wake?.kind, id].filter(Boolean).join(" ") || "唤醒";
      if (typeof wake?.reason !== "string" || !wake.reason.trim() || typeof wake.message !== "string" || !wake.message.trim()) {
        log(`跳过一条缺 reason 或 message 的唤醒（${label}）`);
        continue;
      }
      if (id && seen.has(id)) continue;
      if (id) seen.add(id);
      log(`收到 ${label}`); // 只记类型和编号，不记正文
      enqueue(label, { reason: wake.reason, message: wake.message });
    }
    throw new Error(connected ? "唤醒流断开了（EOF）" : "唤醒流没等到 connected 就断了");
  } finally {
    clearTimeout(idle);
    controller.abort();
    // 已收到的投完再退出：写进流的调用 Leto 已标 accepted，不会重推
    await recovery;
    await queue;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const command = process.argv[2];
  let token = "";
  const scrub = (line) => token ? line.replaceAll(token, "lmt_***") : line;
  try {
    if (command !== "check" && command !== "run") throw new Error("用法：node bridge.mjs check|run");
    const config = loadConfig();
    token = config.token;
    if (command === "check") {
      await check(config);
      console.log("令牌有效，唤醒流握手正常（check 不投递）");
      process.exit(0);
    }
    await run(config, { log: (line) => console.log(scrub(line)) });
  } catch (error) {
    const cause = error?.cause?.code ?? error?.cause?.message; // fetch 只报 fetch failed，原因在 cause 里
    console.error(scrub(`${error?.message ?? error}${cause ? `（${cause}）` : ""}`));
    process.exit(1);
  }
}
