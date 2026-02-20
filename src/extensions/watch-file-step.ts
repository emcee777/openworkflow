/**
 * Watch File Step Type
 *
 * Reusable OW step that polls for a file to appear on the laptop or Hearth.
 * Returns the file's contents when found, or found=false on timeout.
 *
 * Design constraints:
 * - ALL operations are async (no execSync) — OW heartbeats must not be blocked
 * - SSH polling, not fswatch — fswatch can't bridge SSH from Hearth to laptop
 * - Dead worker detection via tmux capture-pane (cheap and reliable)
 * - SSH transient failures handled by the polling loop (next cycle retries)
 * - Consecutive SSH failures tracked — escalates to "ssh_failure" if persistent
 */

import { exec } from "child_process";
import type { StepApi } from "./tracked-step";

const LAPTOP_SSH = "matthewchapin@mac.lan";
const LOCAL_MODE = process.env.OW_LOCAL === "true";
const MAX_CONTENT_BYTES = 100 * 1024; // 100KB truncation limit
const MAX_CONSECUTIVE_SSH_FAILURES = 10; // After this many, assume SSH is down

// --- Interfaces ---

export interface WatchFileInput {
  file_path: string;
  timeout_seconds?: number;         // Default: 3600
  poll_interval_seconds?: number;   // Default: 10
  also_watch_for?: string[];        // Glob patterns for additional files (e.g., "*-ESCALATION.md")
  watch_on?: "laptop" | "hearth";   // Default: "laptop"
  staleness_session_name?: string;  // tmux session to monitor for death
  staleness_timeout_minutes?: number; // Unused currently — death detected via pane content
}

export interface WatchFileOutput {
  found: boolean;
  file_path: string;
  content: string;
  elapsed_seconds: number;
  trigger: "target" | "also_watch";
  reason?: string; // "timeout" | "context_death" | "ssh_failure"
}

// --- Input validation ---

/**
 * Validate that a file path is safe for shell interpolation.
 * Paths must be absolute and contain only safe characters.
 */
function validateFilePath(path: string): void {
  if (!path.startsWith("/")) {
    throw new Error(`File path must be absolute: ${path}`);
  }
  // Reject characters that could cause shell injection
  if (/[`$;|&\n\r\\]/.test(path)) {
    throw new Error(`File path contains unsafe characters: ${path}`);
  }
}

/**
 * Validate that a tmux session name contains only safe characters.
 */
function validateSessionName(name: string): void {
  if (!/^[a-zA-Z0-9_-]+$/.test(name)) {
    throw new Error(`Invalid tmux session name: ${name}`);
  }
}

/**
 * Validate a glob pattern for also_watch_for.
 * Allows path chars + glob wildcards, rejects injection chars.
 */
function validateGlobPattern(pattern: string): void {
  if (/[`$;|&\n\r\\]/.test(pattern)) {
    throw new Error(`Glob pattern contains unsafe characters: ${pattern}`);
  }
}

// --- Internal helpers ---

/**
 * Execute a shell command asynchronously. Returns stdout.
 * Does NOT block the event loop — OW heartbeats continue firing.
 */
function execAsync(cmd: string, timeoutMs: number): Promise<string> {
  return new Promise((resolve, reject) => {
    exec(
      cmd,
      { timeout: timeoutMs, maxBuffer: 50 * 1024 * 1024 },
      (error, stdout, stderr) => {
        if (error) {
          reject(
            new Error(
              `Command failed (code ${error.code}): ${(stderr || "").slice(0, 2000) || error.message}`,
            ),
          );
          return;
        }
        resolve(stdout);
      },
    );
  });
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Truncate content to MAX_CONTENT_BYTES.
 */
function truncateContent(content: string): string {
  if (Buffer.byteLength(content) <= MAX_CONTENT_BYTES) return content;
  const buf = Buffer.from(content);
  return (
    buf.subarray(0, MAX_CONTENT_BYTES).toString("utf-8") +
    "\n...[truncated to 100KB]"
  );
}

// Return type for checkFile — distinguishes "not found" from "SSH error"
interface CheckResult {
  content: string | null; // null = not found
  sshError: boolean;      // true = SSH failed (not just "file not found")
}

/**
 * Check if a file exists and return its contents.
 * Distinguishes between "file not found" and "SSH error" for failure tracking.
 */
async function checkFile(
  filePath: string,
  watchOn: "laptop" | "hearth",
): Promise<CheckResult> {
  try {
    const NOT_FOUND = "__OW_NOT_FOUND__";
    let result: string;

    if (watchOn === "hearth") {
      result = await execAsync(
        `test -f '${filePath}' && cat '${filePath}' || echo '${NOT_FOUND}'`,
        10_000,
      );
    } else if (LOCAL_MODE) {
      result = await execAsync(
        `test -f '${filePath}' && cat '${filePath}' || echo '${NOT_FOUND}'`,
        10_000,
      );
    } else {
      result = await execAsync(
        `ssh ${LAPTOP_SSH} "test -f '${filePath}' && cat '${filePath}' || echo '${NOT_FOUND}'"`,
        30_000,
      );
    }

    if (result.trim() === NOT_FOUND) {
      return { content: null, sshError: false };
    }
    return { content: result, sshError: false };
  } catch {
    return { content: null, sshError: true };
  }
}

/**
 * Check also_watch_for glob patterns. Returns the first matching file path, or null.
 */
async function checkAlsoWatchPatterns(
  patterns: string[],
  watchOn: "laptop" | "hearth",
): Promise<string | null> {
  for (const pattern of patterns) {
    try {
      let result: string;
      if (watchOn === "hearth") {
        result = await execAsync(
          `ls ${pattern} 2>/dev/null | head -1`,
          10_000,
        );
      } else if (LOCAL_MODE) {
        result = await execAsync(
          `ls ${pattern} 2>/dev/null | head -1`,
          10_000,
        );
      } else {
        result = await execAsync(
          `ssh ${LAPTOP_SSH} "ls ${pattern} 2>/dev/null | head -1"`,
          30_000,
        );
      }
      const match = result.trim();
      if (match) return match;
    } catch {
      // Pattern check failed — skip to next pattern
    }
  }
  return null;
}

/**
 * Check if a worker's tmux session has died (context exhaustion or exit).
 *
 * Returns true if:
 * - The tmux session no longer exists (worker exited)
 * - The pane output contains context death markers
 *
 * Returns false if:
 * - The session is alive and active
 * - We can't determine status (SSH failure — assume alive to avoid false positives)
 */
async function isWorkerDead(sessionName: string): Promise<boolean> {
  try {
    const SESSION_GONE = "__OW_SESSION_GONE__";
    let output: string;
    if (LOCAL_MODE) {
      output = await execAsync(
        `tmux capture-pane -t '${sessionName}' -p 2>/dev/null || echo '${SESSION_GONE}'`,
        10_000,
      );
    } else {
      output = await execAsync(
        `ssh ${LAPTOP_SSH} "tmux capture-pane -t '${sessionName}' -p 2>/dev/null || echo '${SESSION_GONE}'"`,
        15_000,
      );
    }

    if (output.includes(SESSION_GONE)) return true;
    if (/context low \(0% remaining\)/i.test(output)) return true;
    if (/context limit reached/i.test(output)) return true;
    return false;
  } catch {
    // Can't check — assume alive. False negative is safer than false positive.
    return false;
  }
}

// --- Exported step function ---

/**
 * Create a watchFile OW step that polls until a file appears.
 *
 * @param step - OW StepApi (for step memoization and heartbeats)
 * @param input - Watch configuration
 * @param stepName - Unique step name (caller MUST include campaign scope)
 * @returns WatchFileOutput with file contents or timeout/death/ssh_failure reason
 *
 * @example
 * ```typescript
 * const result = await createWatchFileStep(step, {
 *   file_path: "/path/to/worker-COMPLETE.yaml",
 *   timeout_seconds: 1800,
 *   poll_interval_seconds: 10,
 *   also_watch_for: ["/path/to/*-ESCALATION.md"],
 *   watch_on: "laptop",
 *   staleness_session_name: "ow-everywhere-1a",
 * }, "watch-eval-v2-worker-a");
 * ```
 */
export function createWatchFileStep(
  step: StepApi,
  input: WatchFileInput,
  stepName: string,
): Promise<WatchFileOutput> {
  // Validate inputs before entering the step
  validateFilePath(input.file_path);
  if (input.staleness_session_name) {
    validateSessionName(input.staleness_session_name);
  }
  if (input.also_watch_for) {
    input.also_watch_for.forEach(validateGlobPattern);
  }

  const filePath = input.file_path;
  const timeoutSeconds = input.timeout_seconds ?? 3600;
  const pollIntervalSeconds = input.poll_interval_seconds ?? 10;
  const alsoWatchFor = input.also_watch_for ?? [];
  const watchOn = input.watch_on ?? "laptop";
  const stalenessSessionName = input.staleness_session_name;
  // Check dead worker every 3rd poll cycle to limit SSH overhead
  const STALENESS_CHECK_FREQUENCY = 3;

  return step.run({ name: stepName }, async () => {
    const startTime = Date.now();
    const timeoutMs = timeoutSeconds * 1000;
    const intervalMs = pollIntervalSeconds * 1000;
    let pollCount = 0;
    let consecutiveSshFailures = 0;

    while (true) {
      const elapsedMs = Date.now() - startTime;

      // --- Timeout check ---
      if (elapsedMs >= timeoutMs) {
        return {
          found: false,
          file_path: filePath,
          content: "",
          elapsed_seconds: Math.round(elapsedMs / 1000),
          trigger: "target" as const,
          reason: "timeout",
        };
      }

      // --- Check target file ---
      const { content: targetContent, sshError } = await checkFile(filePath, watchOn);

      if (targetContent !== null) {
        return {
          found: true,
          file_path: filePath,
          content: truncateContent(targetContent),
          elapsed_seconds: Math.round((Date.now() - startTime) / 1000),
          trigger: "target" as const,
        };
      }

      // Track consecutive SSH failures
      if (sshError) {
        consecutiveSshFailures++;
        if (consecutiveSshFailures >= MAX_CONSECUTIVE_SSH_FAILURES) {
          return {
            found: false,
            file_path: filePath,
            content: "",
            elapsed_seconds: Math.round((Date.now() - startTime) / 1000),
            trigger: "target" as const,
            reason: "ssh_failure",
          };
        }
      } else {
        consecutiveSshFailures = 0;
      }

      // --- Check also_watch_for patterns ---
      if (alsoWatchFor.length > 0) {
        const matchPath = await checkAlsoWatchPatterns(alsoWatchFor, watchOn);
        if (matchPath) {
          let matchContent = "";
          const matchResult = await checkFile(matchPath, watchOn);
          matchContent = matchResult.content
            ? truncateContent(matchResult.content)
            : "";
          return {
            found: true,
            file_path: matchPath,
            content: matchContent,
            elapsed_seconds: Math.round((Date.now() - startTime) / 1000),
            trigger: "also_watch" as const,
          };
        }
      }

      // --- Dead worker detection (every Nth poll to limit SSH overhead) ---
      if (
        stalenessSessionName &&
        pollCount % STALENESS_CHECK_FREQUENCY === 0
      ) {
        const dead = await isWorkerDead(stalenessSessionName);
        if (dead) {
          return {
            found: false,
            file_path: filePath,
            content: "",
            elapsed_seconds: Math.round((Date.now() - startTime) / 1000),
            trigger: "target" as const,
            reason: "context_death",
          };
        }
      }

      pollCount++;
      await delay(intervalMs);
    }
  });
}
