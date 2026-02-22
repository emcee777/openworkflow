/**
 * Dispatch Worker Step Type
 *
 * A reusable OW step that dispatches a Claude Code worker session via tmux
 * and waits for its COMPLETE.yaml. Core building block for automated campaign
 * orchestration through OpenWorkflow.
 *
 * Runs on The Hearth (Mac Mini). SSHes to the laptop (mac.lan) for:
 *   - tmux session creation via dispatch-with-ritual.sh
 *   - COMPLETE.yaml / ESCALATION.md file watching
 *   - Post-dispatch verification
 *
 * ALL operations are async (no execSync) to avoid blocking OW heartbeats.
 * SSH calls retry with exponential backoff (5s, 15s, 45s).
 */

import { exec } from "node:child_process";
import { parse as parseYaml } from "yaml";
import type { StepApi } from "./tracked-step.js";

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

export interface DispatchWorkerInput {
  session_name: string;
  prompt_path: string; // Absolute path on laptop
  launcher: string; // "cpb" | "cbb" | "cmb" | "clb" | "c5b" | "local-glm" | "local-dispatch"
  complete_file: string; // Absolute path on laptop
  timeout_seconds: number; // Default: 3600
  ritual: boolean; // Default: true
  post_dispatch_verify: boolean; // Default: true
  context_file?: string; // Optional path to supplementary context file
  // camelCase aliases (PostgreSQL toCamel transform)
  sessionName?: string;
  promptPath?: string;
  completeFile?: string;
  timeoutSeconds?: number;
  postDispatchVerify?: boolean;
  contextFile?: string;
}

export interface DispatchWorkerOutput {
  status:
    | "completed"
    | "timeout"
    | "escalation"
    | "failed_to_start"
    | "ssh_failure";
  complete_yaml: Record<string, unknown> | null;
  escalation_content: string | null;
  elapsed_seconds: number;
  session_name: string;
  death_detected: boolean;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LAPTOP_SSH = "matthewchapin@mac.lan";

/** Launcher paths use $HOME for remote shell expansion */
const LAUNCHERS: Record<string, string> = {
  cpb: "$HOME/.claude/scripts/launchers/cpb.sh",
  cbb: "$HOME/.claude/scripts/launchers/cbb.sh",
  cmb: "$HOME/.claude/scripts/launchers/cmb.sh",
  clb: "$HOME/.claude/scripts/launchers/clb.sh",
  c5b: "$HOME/.claude/scripts/launchers/c5b.sh",
  "local-glm": "$HOME/.claude/scripts/tools/local-task-dispatch.sh",
  "local-dispatch": "$HOME/.claude/scripts/tools/local-task-dispatch.sh",
};

/** Local launcher → Ollama model mapping. Keys must match LAUNCHERS. */
const LOCAL_LAUNCHER_MODELS: Record<string, string> = {
  "local-glm": "glm-4.7-flash",
  "local-dispatch": "glm-4.7-flash",
};

const DISPATCH_SCRIPT = "$HOME/.claude/scripts/dispatch-with-ritual.sh";

/** When true, run commands locally instead of via SSH */
const LOCAL_MODE = process.env.OW_LOCAL === "true";

/** SSH retry delays: 5s, 15s, 45s */
const SSH_RETRY_DELAYS_MS = [5_000, 15_000, 45_000];

/** Polling interval for COMPLETE.yaml check */
const POLL_INTERVAL_MS = 10_000;

/** Session name validation: alphanumeric, dashes, underscores */
const SESSION_NAME_RE = /^[a-zA-Z0-9_-]+$/;

/** Cloud launchers available for rate-limit failover rotation */
const CLOUD_LAUNCHERS = ["cpb", "cbb", "cmb", "clb", "c5b"];

/** Rate-limit text patterns in tmux pane output (case-insensitive) */
const RATE_LIMIT_RE =
  /rate.?limit|capacity|exceeded|too many requests|429|overloaded|throttl/i;

/** Max rate-limit retries across different accounts */
const MAX_RATE_LIMIT_RETRIES = 3;

/** Delay after dispatch before checking for rate-limit (seconds) */
const RATE_LIMIT_CHECK_DELAY_MS = 35_000;

/** Exponential backoff delays between rate-limit retries (ms): 10s, 30s, 90s */
const RATE_LIMIT_BACKOFF_MS = [10_000, 30_000, 90_000];

/**
 * In-memory cache of recently rate-limited launchers.
 * Maps launcher name → timestamp when cooldown expires.
 * Survives across dispatch calls within the same worker process.
 */
const rateLimitedUntil = new Map<string, number>();

/** Cooldown duration for rate-limited launchers (5 minutes) */
const RATE_LIMIT_COOLDOWN_MS = 5 * 60 * 1000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Dual-convention accessor for PostgreSQL camelCase-transformed inputs.
 * Tries snake_case first, then camelCase.
 */
function get<T>(obj: Record<string, unknown>, snakeKey: string): T | undefined {
  if (obj[snakeKey] !== undefined) return obj[snakeKey] as T;
  const camelKey = snakeKey.replace(/_([a-z])/g, (_, l: string) =>
    l.toUpperCase(),
  );
  if (obj[camelKey] !== undefined) return obj[camelKey] as T;
  return undefined;
}

/**
 * Async shell exec. Does NOT block the event loop — OW heartbeats continue.
 */
function execAsync(cmd: string, timeoutMs: number): Promise<string> {
  return new Promise((resolve, reject) => {
    exec(
      cmd,
      { timeout: timeoutMs, maxBuffer: 50 * 1024 * 1024 },
      (error, stdout, stderr) => {
        if (error) {
          const msg = (stderr || error.message || "unknown error").slice(
            0,
            2000,
          );
          reject(new Error(`Command failed (code ${error.code ?? "?"}): ${msg}`));
          return;
        }
        resolve(stdout.trim());
      },
    );
  });
}

/**
 * Execute a command on the laptop via SSH with retry + exponential backoff.
 *
 * The command is wrapped in single quotes so that:
 *   - The local shell passes it verbatim to SSH
 *   - The remote shell expands $HOME, etc.
 *   - Single quotes in cmd are escaped via the standard '\'' pattern
 */
async function sshExec(
  cmd: string,
  timeoutMs: number,
  label: string,
): Promise<string> {
  if (LOCAL_MODE) {
    // Use single quotes to prevent the outer sh from expanding $variables
    const escaped = cmd.replace(/'/g, "'\\''");
    return execAsync(`bash -c '${escaped}'`, timeoutMs);
  }

  const escaped = cmd.replace(/'/g, "'\\''");
  const fullCmd = `ssh ${LAPTOP_SSH} '${escaped}'`;

  let lastError: Error | null = null;
  for (let attempt = 0; attempt <= SSH_RETRY_DELAYS_MS.length; attempt++) {
    try {
      return await execAsync(fullCmd, timeoutMs);
    } catch (error) {
      lastError = error as Error;
      if (attempt < SSH_RETRY_DELAYS_MS.length) {
        const delay = SSH_RETRY_DELAYS_MS[attempt];
        console.log(
          `[dispatch] SSH retry ${attempt + 1}/${SSH_RETRY_DELAYS_MS.length} for ${label} (waiting ${delay / 1000}s): ${lastError.message.slice(0, 200)}`,
        );
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }
  throw lastError ?? new Error(`SSH failed: ${label}`);
}

/**
 * Single-attempt SSH exec for polling operations.
 * No retry — the outer polling loop handles recovery on the next iteration.
 * This avoids 65s+ retry delays on each poll cycle during SSH outages.
 */
async function sshPoll(cmd: string, timeoutMs: number): Promise<string> {
  if (LOCAL_MODE) {
    const escaped = cmd.replace(/'/g, "'\\''");
    return execAsync(`bash -c '${escaped}'`, timeoutMs);
  }
  const escaped = cmd.replace(/'/g, "'\\''");
  return execAsync(`ssh ${LAPTOP_SSH} '${escaped}'`, timeoutMs);
}

/** Non-blocking sleep */
function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Check worker_runs table for a session's completion status.
 * Returns 'complete' if the worker_runs entry shows completion, null otherwise.
 * Uses local psql (runs on Hearth where the DB is).
 */
async function checkWorkerRunsStatus(sessionName: string): Promise<string | null> {
  try {
    const dbName = process.env.DB_NAME || "hearth";
    const result = await execAsync(
      `/opt/homebrew/opt/postgresql@16/bin/psql -h localhost -U claude_daemon -d ${dbName} -t -A -c "SELECT status FROM public.worker_runs WHERE session_name = '${sessionName.replace(/'/g, "''")}' LIMIT 1"`,
      10_000,
    );
    const status = result.trim();
    return status || null;
  } catch {
    // DB query failure is non-fatal — continue relying on COMPLETE.yaml
    return null;
  }
}

// ---------------------------------------------------------------------------
// Main Export
// ---------------------------------------------------------------------------

/**
 * Create a dispatch worker step within an OW workflow.
 *
 * @param step     - StepApi from the workflow context (or a trackedStep wrapper)
 * @param input    - Dispatch configuration
 * @param stepName - Caller-provided step name for memoization uniqueness.
 *                   Convention: `dispatch-{campaign}-{worker.id}`
 *
 * @example
 * ```ts
 * const result = await createDispatchStep(step, {
 *   session_name: "eval-v2-worker-a",
 *   prompt_path: "/Users/matthewchapin/.claude/campaigns/.../worker-a.md",
 *   launcher: "cpb",
 *   complete_file: "/Users/matthewchapin/.claude/campaigns/.../worker-a-COMPLETE.yaml",
 *   timeout_seconds: 3600,
 *   ritual: true,
 *   post_dispatch_verify: true,
 * }, "dispatch-eval-v2-worker-a");
 * ```
 */
export function createDispatchStep(
  step: StepApi,
  input: DispatchWorkerInput,
  stepName: string,
): Promise<DispatchWorkerOutput> {
  return step.run({ name: stepName }, async () => {
    const startTime = Date.now();
    const elapsedSec = () => (Date.now() - startTime) / 1000;

    // --- Resolve inputs (camelCase fallback) ---
    const sessionName = get<string>(input, "session_name") ?? "";
    const promptPath = get<string>(input, "prompt_path") ?? "";
    const launcher = get<string>(input, "launcher") ?? "cpb";
    const completeFile = get<string>(input, "complete_file") ?? "";
    const timeoutSeconds = get<number>(input, "timeout_seconds") ?? 3600;
    const ritual = input.ritual ?? true;
    const postDispatchVerify =
      get<boolean>(input, "post_dispatch_verify") ?? true;

    // --- Validate ---
    if (!sessionName || !promptPath || !completeFile) {
      throw new Error(
        `Missing required fields: session_name="${sessionName}", prompt_path="${promptPath}", complete_file="${completeFile}"`,
      );
    }

    if (!SESSION_NAME_RE.test(sessionName)) {
      throw new Error(
        `Invalid session_name: "${sessionName}". Must match ${SESSION_NAME_RE.source}`,
      );
    }

    const launcherPath = LAUNCHERS[launcher];
    if (!launcherPath) {
      throw new Error(
        `Unknown launcher: "${launcher}". Valid: ${Object.keys(LAUNCHERS).join(", ")}`,
      );
    }

    console.log(
      `[dispatch] Starting: ${sessionName} (launcher=${launcher}, ritual=${ritual}, timeout=${timeoutSeconds}s)`,
    );

    // -----------------------------------------------------------------------
    // Phase 1: Pre-checks
    // -----------------------------------------------------------------------

    // Check for existing tmux session (skip for local launchers — they don't use tmux)
    if (!(launcher in LOCAL_LAUNCHER_MODELS)) {
      try {
        const check = await sshExec(
          `tmux has-session -t "=${sessionName}" 2>/dev/null && echo EXISTS || echo OK`,
          30_000,
          `check-session-${sessionName}`,
        );
        if (check.includes("EXISTS")) {
          console.error(
            `[dispatch] Session "${sessionName}" already exists`,
          );
          return {
            status: "failed_to_start",
            complete_yaml: null,
            escalation_content: `tmux session "${sessionName}" already exists`,
            elapsed_seconds: elapsedSec(),
            session_name: sessionName,
            death_detected: false,
          };
        }
      } catch (error) {
        console.error(
          `[dispatch] SSH failed during session check: ${(error as Error).message.slice(0, 300)}`,
        );
        return {
          status: "ssh_failure",
          complete_yaml: null,
          escalation_content: (error as Error).message,
          elapsed_seconds: elapsedSec(),
          session_name: sessionName,
          death_detected: false,
        };
      }
    }

    // Check prompt file exists
    try {
      await sshExec(
        `test -f "${promptPath}"`,
        15_000,
        `check-prompt-${sessionName}`,
      );
    } catch {
      return {
        status: "failed_to_start",
        complete_yaml: null,
        escalation_content: `Prompt file not found on laptop: ${promptPath}`,
        elapsed_seconds: elapsedSec(),
        session_name: sessionName,
        death_detected: false,
      };
    }

    // Check if COMPLETE.yaml already exists (from a prior run)
    try {
      const preCheck = await sshExec(
        `test -f "${completeFile}" && echo EXISTS || echo NO`,
        15_000,
        `precheck-complete-${sessionName}`,
      );
      if (preCheck.includes("EXISTS")) {
        console.log(
          `[dispatch] COMPLETE.yaml already exists for ${sessionName} — reading`,
        );
        const content = await sshExec(
          `cat "${completeFile}"`,
          30_000,
          `read-preexisting-complete-${sessionName}`,
        );
        let parsed: Record<string, unknown>;
        try {
          parsed = parseYaml(content) as Record<string, unknown>;
        } catch {
          parsed = { raw_content: content };
        }
        return {
          status: "completed",
          complete_yaml: parsed,
          escalation_content: null,
          elapsed_seconds: elapsedSec(),
          session_name: sessionName,
          death_detected: false,
        };
      }
    } catch {
      // Non-fatal — continue to dispatch
    }

    // -----------------------------------------------------------------------
    // Phase 2+3: Dispatch with rate-limit detection and auto-redispatch
    // -----------------------------------------------------------------------

    const isLocalLauncher = launcher in LOCAL_LAUNCHER_MODELS;
    let activeLauncher = launcher;
    let activeLauncherPath = launcherPath;
    const triedLaunchers = new Set<string>([launcher]);

    if (isLocalLauncher) {
      // Local model dispatch: no rate-limit risk, no retry needed
      try {
        const model = LOCAL_LAUNCHER_MODELS[launcher];
        const completeDir = completeFile.substring(0, completeFile.lastIndexOf("/"));
        await sshExec(
          `mkdir -p "${completeDir}" && ${activeLauncherPath} --model ${model} --prompt-file "${promptPath}" --output-dir "/tmp/local-${sessionName}" --task-id "${sessionName}" && cp "/tmp/local-${sessionName}/COMPLETE.yaml" "${completeFile}"`,
          300_000,
          `local-dispatch-${sessionName}`,
        );
        console.log(`[dispatch] Local dispatch complete: ${sessionName} (model=${model})`);
      } catch (error) {
        console.error(
          `[dispatch] Local dispatch failed for ${sessionName}: ${(error as Error).message.slice(0, 300)}`,
        );
        return {
          status: "ssh_failure",
          complete_yaml: null,
          escalation_content: (error as Error).message,
          elapsed_seconds: elapsedSec(),
          session_name: sessionName,
          death_detected: false,
        };
      }
    } else {
      // Cloud dispatch with rate-limit retry loop
      let dispatched = false;

      // Pre-check: skip launchers known to be rate-limited
      const now = Date.now();
      if (rateLimitedUntil.has(activeLauncher) && rateLimitedUntil.get(activeLauncher)! > now) {
        const remaining = Math.round((rateLimitedUntil.get(activeLauncher)! - now) / 1000);
        console.log(
          `[dispatch] Pre-check: ${activeLauncher} is rate-limited for ${remaining}s more — finding alternative`,
        );
        const altLauncher = CLOUD_LAUNCHERS.find(
          (l) => !triedLaunchers.has(l) && (!rateLimitedUntil.has(l) || rateLimitedUntil.get(l)! <= now),
        );
        if (altLauncher) {
          activeLauncher = altLauncher;
          activeLauncherPath = LAUNCHERS[altLauncher]!;
          triedLaunchers.add(altLauncher);
          console.log(`[dispatch] Pre-check: switched to ${activeLauncher}`);
        } else {
          console.warn(`[dispatch] Pre-check: all known launchers rate-limited, proceeding with ${activeLauncher}`);
        }
      }

      for (let attempt = 0; attempt <= MAX_RATE_LIMIT_RETRIES; attempt++) {
        if (attempt > 0) {
          // Exponential backoff between rate-limit retries
          const backoffMs = RATE_LIMIT_BACKOFF_MS[Math.min(attempt - 1, RATE_LIMIT_BACKOFF_MS.length - 1)];
          console.log(
            `[dispatch] Rate-limit retry ${attempt}/${MAX_RATE_LIMIT_RETRIES} for ${sessionName} (launcher=${activeLauncher}, backoff=${backoffMs / 1000}s)`,
          );
          await sleep(backoffMs);
        }

        // --- Dispatch ---
        try {
          if (ritual) {
            const owRunId = process.env.OW_RUN_ID ?? "ow-dispatch";
            const contextFile = get<string>(input, "context_file") ?? "";
            const contextArg = contextFile ? ` --context-file "${contextFile}"` : "";
            await sshExec(
              `OW_RUN_ID=${owRunId} ${DISPATCH_SCRIPT} "${sessionName}" "${promptPath}" ${activeLauncherPath}${contextArg}`,
              180_000,
              `dispatch-ritual-${sessionName}-attempt${attempt}`,
            );
            console.log(`[dispatch] Ritual dispatch complete: ${sessionName} (attempt ${attempt})`);
          } else {
            console.log(`[dispatch] No-ritual dispatch: ${sessionName}`);
            await sshExec(
              `tmux new-session -d -s "${sessionName}" ${activeLauncherPath}`,
              30_000,
              `create-session-${sessionName}`,
            );
            await sleep(20_000);
            const dispatchMsg = `Welcome to the team, brother. Read ${promptPath} and execute. Ask any questions. No quick hacks. If you hit any hiccups PAUSE and come ask me. After implementation, run extraction per session-end protocol.`;
            await sshExec(
              `tmux send-keys -t "${sessionName}" "${dispatchMsg}" Enter`,
              30_000,
              `send-prompt-${sessionName}`,
            );
            await sleep(3_000);
            await sshExec(
              `tmux send-keys -t "${sessionName}" Enter`,
              10_000,
              `confirm-${sessionName}`,
            );
            console.log(`[dispatch] No-ritual dispatch complete: ${sessionName}`);
          }
        } catch (error) {
          console.error(
            `[dispatch] Dispatch failed for ${sessionName}: ${(error as Error).message.slice(0, 300)}`,
          );
          return {
            status: "ssh_failure",
            complete_yaml: null,
            escalation_content: (error as Error).message,
            elapsed_seconds: elapsedSec(),
            session_name: sessionName,
            death_detected: false,
          };
        }

        // --- Post-dispatch: check for rate-limit ---
        if (postDispatchVerify) {
          await sleep(RATE_LIMIT_CHECK_DELAY_MS);
          try {
            const paneContent = await sshExec(
              `tmux capture-pane -t "${sessionName}" -p -S -30 2>/dev/null || echo ""`,
              30_000,
              `verify-ratelimit-${sessionName}-attempt${attempt}`,
            );
            console.log(
              `[dispatch] Post-verify ${sessionName}: ${paneContent.slice(0, 200)}`,
            );

            // Check for rate-limit patterns
            if (RATE_LIMIT_RE.test(paneContent)) {
              console.warn(
                `[dispatch] RATE LIMIT detected for ${sessionName} on ${activeLauncher} — killing session`,
              );

              // Record this launcher as rate-limited for future pre-checks
              rateLimitedUntil.set(activeLauncher, Date.now() + RATE_LIMIT_COOLDOWN_MS);

              // Kill the rate-limited session
              try {
                await sshExec(
                  `tmux kill-session -t "=${sessionName}" 2>/dev/null || true`,
                  15_000,
                  `kill-ratelimited-${sessionName}`,
                );
              } catch {
                // Non-fatal — session may already be gone
              }

              // Find next available cloud launcher (prefer non-rate-limited ones)
              const nextNow = Date.now();
              const nextLauncher = CLOUD_LAUNCHERS.find(
                (l) => !triedLaunchers.has(l) && (!rateLimitedUntil.has(l) || rateLimitedUntil.get(l)! <= nextNow),
              ) ?? CLOUD_LAUNCHERS.find(
                (l) => !triedLaunchers.has(l),
              );
              if (!nextLauncher || attempt >= MAX_RATE_LIMIT_RETRIES) {
                console.error(
                  `[dispatch] All accounts exhausted or max retries reached for ${sessionName}. Tried: ${[...triedLaunchers].join(", ")}`,
                );
                return {
                  status: "failed_to_start",
                  complete_yaml: null,
                  escalation_content: `Rate-limited on all tried accounts: ${[...triedLaunchers].join(", ")}`,
                  elapsed_seconds: elapsedSec(),
                  session_name: sessionName,
                  death_detected: false,
                };
              }

              // Switch to next launcher
              activeLauncher = nextLauncher;
              activeLauncherPath = LAUNCHERS[nextLauncher]!;
              triedLaunchers.add(nextLauncher);
              console.log(
                `[dispatch] Switching to ${activeLauncher} for retry`,
              );

              // Backoff is handled at top of loop via RATE_LIMIT_BACKOFF_MS
              continue; // Next iteration of retry loop
            }

            // If prompt appears stuck in input line, re-send Enter
            if (paneContent.includes("Welcome to the team")) {
              console.log(
                `[dispatch] Prompt may be stuck — re-sending Enter for ${sessionName}`,
              );
              await sshExec(
                `tmux send-keys -t "${sessionName}" Enter`,
                10_000,
                `resend-enter-${sessionName}`,
              );
            }
          } catch (error) {
            // Non-fatal — worker might still be initializing
            console.warn(
              `[dispatch] Post-verify warning for ${sessionName}: ${(error as Error).message.slice(0, 200)}`,
            );
          }
        }

        // Dispatch succeeded (no rate-limit detected)
        dispatched = true;
        break;
      }

      if (!dispatched) {
        return {
          status: "failed_to_start",
          complete_yaml: null,
          escalation_content: `Failed to dispatch after ${MAX_RATE_LIMIT_RETRIES} rate-limit retries`,
          elapsed_seconds: elapsedSec(),
          session_name: sessionName,
          death_detected: false,
        };
      }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Wait for COMPLETE.yaml
    // TODO: replace with watchFile step when available (Worker 1C)
    // -----------------------------------------------------------------------

    console.log(
      `[dispatch] Watching for ${completeFile} (timeout: ${timeoutSeconds}s, poll: ${POLL_INTERVAL_MS / 1000}s)`,
    );

    const completeDir = completeFile.substring(
      0,
      completeFile.lastIndexOf("/"),
    );
    const timeoutMs = timeoutSeconds * 1000;

    while (Date.now() - startTime < timeoutMs) {
      // --- Check for COMPLETE.yaml ---
      // Uses sshPoll (single attempt) — outer loop handles recovery
      try {
        const exists = await sshPoll(
          `test -f "${completeFile}" && echo EXISTS || echo NO`,
          30_000,
        );
        if (exists.includes("EXISTS")) {
          // File found — use sshExec with retry for the critical read
          const content = await sshExec(
            `cat "${completeFile}"`,
            30_000,
            `read-complete-${sessionName}`,
          );
          let parsed: Record<string, unknown>;
          try {
            parsed = parseYaml(content) as Record<string, unknown>;
          } catch {
            parsed = { raw_content: content };
          }
          console.log(
            `[dispatch] COMPLETE: ${sessionName} (${elapsedSec().toFixed(0)}s)`,
          );
          return {
            status: "completed",
            complete_yaml: parsed,
            escalation_content: null,
            elapsed_seconds: elapsedSec(),
            session_name: sessionName,
            death_detected: false,
          };
        }
      } catch {
        // SSH failure during poll — continue to next iteration
      }

      // --- Check worker_runs table (secondary completion signal) ---
      // If a worker wrote to worker_runs but failed to write COMPLETE.yaml,
      // this catches the completion that would otherwise be missed.
      {
        const wrStatus = await checkWorkerRunsStatus(sessionName);
        if (wrStatus === "complete") {
          console.log(
            `[dispatch] worker_runs shows complete for ${sessionName} — checking COMPLETE.yaml one more time`,
          );
          // Try to read COMPLETE.yaml (it may have been written after our last check)
          try {
            const content = await sshExec(
              `cat "${completeFile}" 2>/dev/null || echo ""`,
              30_000,
              `read-complete-workerruns-${sessionName}`,
            );
            let parsed: Record<string, unknown>;
            if (content) {
              try {
                parsed = parseYaml(content) as Record<string, unknown>;
              } catch {
                parsed = { raw_content: content };
              }
            } else {
              parsed = { source: "worker_runs", status: "complete" };
            }
            console.log(
              `[dispatch] COMPLETE (via worker_runs): ${sessionName} (${elapsedSec().toFixed(0)}s)`,
            );
            return {
              status: "completed",
              complete_yaml: parsed,
              escalation_content: null,
              elapsed_seconds: elapsedSec(),
              session_name: sessionName,
              death_detected: false,
            };
          } catch {
            // COMPLETE.yaml read failed but worker_runs says complete —
            // return completed with minimal info
            return {
              status: "completed",
              complete_yaml: { source: "worker_runs", status: "complete" },
              escalation_content: null,
              elapsed_seconds: elapsedSec(),
              session_name: sessionName,
              death_detected: false,
            };
          }
        }
      }

      // --- Check for ESCALATION.md ---
      try {
        const escalation = await sshPoll(
          `find "${completeDir}" -maxdepth 1 -name "*-ESCALATION.md" -type f 2>/dev/null | head -1`,
          30_000,
        );
        if (escalation) {
          const content = await sshExec(
            `cat "${escalation}"`,
            30_000,
            `read-escalation-${sessionName}`,
          );
          console.log(`[dispatch] ESCALATION: ${sessionName}`);
          return {
            status: "escalation",
            complete_yaml: null,
            escalation_content: content,
            elapsed_seconds: elapsedSec(),
            session_name: sessionName,
            death_detected: false,
          };
        }
      } catch {
        // Continue
      }

      // --- Check for session death ---
      try {
        const alive = await sshPoll(
          `tmux has-session -t "=${sessionName}" 2>/dev/null && echo ALIVE || echo DEAD`,
          30_000,
        );
        if (alive.includes("DEAD")) {
          // Session died — use sshExec with retry for the critical final check
          try {
            const lastCheck = await sshExec(
              `test -f "${completeFile}" && echo EXISTS || echo NO`,
              30_000,
              `death-check-complete-${sessionName}`,
            );
            if (lastCheck.includes("EXISTS")) {
              const content = await sshExec(
                `cat "${completeFile}"`,
                30_000,
                `read-complete-postdeath-${sessionName}`,
              );
              let parsed: Record<string, unknown>;
              try {
                parsed = parseYaml(content) as Record<string, unknown>;
              } catch {
                parsed = { raw_content: content };
              }
              console.log(
                `[dispatch] COMPLETE (post-death): ${sessionName} (${elapsedSec().toFixed(0)}s)`,
              );
              return {
                status: "completed",
                complete_yaml: parsed,
                escalation_content: null,
                elapsed_seconds: elapsedSec(),
                session_name: sessionName,
                death_detected: true,
              };
            }
          } catch {
            // Fall through to death report
          }

          console.log(
            `[dispatch] DEATH: ${sessionName} — session gone, no COMPLETE.yaml`,
          );
          return {
            status: "timeout",
            complete_yaml: null,
            escalation_content: `Worker session "${sessionName}" died (context limit or crash) without writing COMPLETE.yaml`,
            elapsed_seconds: elapsedSec(),
            session_name: sessionName,
            death_detected: true,
          };
        }
      } catch {
        // SSH failed checking session status — continue polling
      }

      // Wait before next poll
      await sleep(POLL_INTERVAL_MS);
    }

    // --- Timeout: final COMPLETE.yaml check before declaring timeout ---
    // Worker may have finished in the last poll interval
    try {
      const finalCheck = await sshPoll(
        `test -f "${completeFile}" && echo EXISTS || echo NO`,
        30_000,
      );
      if (finalCheck.includes("EXISTS")) {
        const content = await sshExec(
          `cat "${completeFile}"`,
          30_000,
          `read-complete-pretimeout-${sessionName}`,
        );
        let parsed: Record<string, unknown>;
        try {
          parsed = parseYaml(content) as Record<string, unknown>;
        } catch {
          parsed = { raw_content: content };
        }
        console.log(
          `[dispatch] COMPLETE (pre-timeout): ${sessionName} (${elapsedSec().toFixed(0)}s)`,
        );
        return {
          status: "completed",
          complete_yaml: parsed,
          escalation_content: null,
          elapsed_seconds: elapsedSec(),
          session_name: sessionName,
          death_detected: false,
        };
      }
    } catch {
      // SSH failed — proceed with timeout
    }

    console.log(
      `[dispatch] TIMEOUT: ${sessionName} after ${timeoutSeconds}s`,
    );
    return {
      status: "timeout",
      complete_yaml: null,
      escalation_content: null,
      elapsed_seconds: elapsedSec(),
      session_name: sessionName,
      death_detected: false,
    };
  });
}
