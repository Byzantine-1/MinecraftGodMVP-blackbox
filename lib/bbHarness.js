const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { spawn, spawnSync } = require("child_process");

const BLACKBOX_ROOT = path.resolve(__dirname, "..");
const CONFIG_PATH = path.join(BLACKBOX_ROOT, "blackbox.config.json");
const DEFAULT_PUBLIC_REPO = path.resolve(BLACKBOX_ROOT, "..", "minecraft-god-mvp");

function loadBlackboxConfig() {
  if (!fs.existsSync(CONFIG_PATH)) return {};
  try {
    return JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
  } catch {
    return {};
  }
}

const BLACKBOX_CONFIG = loadBlackboxConfig();

// Where to find the public repo.
function publicRepoDir() {
  if (process.env.PUBLIC_REPO_DIR) return path.resolve(process.env.PUBLIC_REPO_DIR);
  if (BLACKBOX_CONFIG.publicRepoPath) return path.resolve(BLACKBOX_CONFIG.publicRepoPath);
  return DEFAULT_PUBLIC_REPO;
}

// Find CLI entry file.
function findCliEntry(repoDir) {
  const candidates = ["index.js", path.join("src", "index.js")].map((p) =>
    path.join(repoDir, p)
  );
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  throw new Error(`Could not find CLI entry. Looked for: ${candidates.join(", ")}`);
}

function safeStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
  return p;
}

// Blackbox artifacts folder.
function makeRunDir(testName) {
  const dir = path.join(BLACKBOX_ROOT, "artifacts", `${testName}-${safeStamp()}`);
  ensureDir(dir);
  return dir;
}

// Optional override keeps backwards compatibility for callers that still use runDir.
function memoryFileFor(runDir, resolvedPath = null) {
  return resolvedPath ? path.resolve(resolvedPath) : path.join(runDir, "memory.json");
}

function resolveMemoryFile(repoDir, entry) {
  const candidates = [
    process.env.PUBLIC_MEMORY_FILE ? path.resolve(process.env.PUBLIC_MEMORY_FILE) : null,
    path.join(path.dirname(entry), "memory.json"),
    path.join(repoDir, "src", "memory.json"),
    path.join(repoDir, "memory.json"),
  ].filter(Boolean);

  const uniqueCandidates = [...new Set(candidates)];
  for (const candidate of uniqueCandidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return uniqueCandidates[0];
}

function staleLockThresholdMs() {
  const raw = Number(process.env.BB_STALE_LOCK_MS || 300);
  if (!Number.isFinite(raw) || raw < 0) return 300;
  return raw;
}

function clearStaleLock(memFile, { staleAfterMs = staleLockThresholdMs() } = {}) {
  const lockPath = `${memFile}.lock`;
  if (!fs.existsSync(lockPath)) {
    return { lockPath, existed: false, removed: false, ageMs: 0 };
  }

  let ageMs = 0;
  try {
    ageMs = Date.now() - fs.statSync(lockPath).mtimeMs;
  } catch {
    // If stat fails, fall through and attempt an unlink below.
  }

  if (ageMs < staleAfterMs) {
    return { lockPath, existed: true, removed: false, ageMs };
  }

  try {
    fs.unlinkSync(lockPath);
    return { lockPath, existed: true, removed: true, ageMs };
  } catch (error) {
    return {
      lockPath,
      existed: true,
      removed: false,
      ageMs,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

// Your engine may use a different env var; we set several common ones.
const MEM_ENV_KEYS = [
  "MEMORY_FILE",
  "MEMORY_PATH",
  "GOD_MEMORY_FILE",
  "MINECRAFT_GOD_MEMORY_FILE",
];

function sha256File(p) {
  if (!fs.existsSync(p)) return null;
  const buf = fs.readFileSync(p);
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function writeText(runDir, name, text) {
  fs.writeFileSync(path.join(runDir, name), text, "utf8");
}

async function waitForCondition(fn, { timeoutMs = 8000, pollMs = 25, label = "condition" } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const value = await fn();
    if (value) return value;
    await new Promise((r) => setTimeout(r, pollMs));
  }
  throw new Error(`Timeout waiting for ${label}.`);
}

function waitForFileHash(filePath, opts = {}) {
  return waitForCondition(() => sha256File(filePath), {
    timeoutMs: opts.timeoutMs ?? 8000,
    pollMs: opts.pollMs ?? 40,
    label: `file hash: ${filePath}`,
  });
}

function forceKill(child) {
  if (!child || child.killed) return;

  try {
    // Try hard kill first (works on unix).
    child.kill("SIGKILL");
    return;
  } catch {}

  try {
    // Fallback: normal kill.
    child.kill("SIGTERM");
  } catch {}

  // Windows fallback: taskkill /F.
  if (process.platform === "win32" && child.pid) {
    spawnSync("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
      stdio: "ignore",
    });
  }
}

function startCli({ runDir, extraEnv = {} }) {
  const repoDir = publicRepoDir();
  const entry = findCliEntry(repoDir);

  const resolvedMemFile = resolveMemoryFile(repoDir, entry);
  const memFile = memoryFileFor(runDir, resolvedMemFile);
  const lockInfo = clearStaleLock(memFile);

  const env = { ...process.env, ...extraEnv, NODE_ENV: "test" };
  for (const k of MEM_ENV_KEYS) env[k] = memFile;

  // Some projects expect cwd at repo root.
  const child = spawn(process.execPath, [entry], {
    cwd: repoDir,
    env,
    stdio: ["pipe", "pipe", "pipe"],
  });

  let stdout = "";
  let stderr = "";

  child.stdout.on("data", (d) => {
    stdout += d.toString("utf8");
  });
  child.stderr.on("data", (d) => {
    stderr += d.toString("utf8");
  });

  function send(line) {
    if (child.stdin.destroyed) return;
    child.stdin.write(line.endsWith("\n") ? line : `${line}\n`);
  }

  function snapshot() {
    return { stdout, stderr };
  }

  async function waitFor(re, { timeoutMs = 8000 } = {}) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (re.test(stdout) || re.test(stderr)) return true;
      await new Promise((r) => setTimeout(r, 25));
    }
    const s = snapshot();
    throw new Error(`Timeout waiting for ${re}. stdout:\n${s.stdout}\n\nstderr:\n${s.stderr}`);
  }

  async function stopGraceful({ timeoutMs = 4000 } = {}) {
    try {
      send("exit");
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        if (child.killed) break;
        if (child.exitCode !== null) break;
        await new Promise((r) => setTimeout(r, 25));
      }
    } finally {
      forceKill(child);
    }
  }

  return {
    child,
    repoDir,
    entry,
    runDir,
    memFile,
    lockInfo,
    send,
    waitFor,
    waitForFileHash: (opts = {}) => waitForFileHash(memFile, opts),
    snapshot,
    stopGraceful,
  };
}

module.exports = {
  publicRepoDir,
  findCliEntry,
  makeRunDir,
  memoryFileFor,
  resolveMemoryFile,
  clearStaleLock,
  sha256File,
  waitForFileHash,
  writeText,
  startCli,
  forceKill,
};
