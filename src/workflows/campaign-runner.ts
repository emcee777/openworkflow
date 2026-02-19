/**
 * Campaign Runner Workflow
 *
 * Reads a campaign specification YAML and executes it as a series of OW steps:
 * preflight checks, worker dispatches (sequential or parallel), gate checks,
 * and campaign close with COMPLETE.yaml aggregation.
 *
 * This is the workflow behind `ow campaign start <spec.yaml>`.
 *
 * Key design:
 * - Sequential phases use createDispatchStep (dispatch + wait per worker)
 * - Parallel phases use custom dispatch-all + watch-all steps
 * - All SSH calls are async to avoid blocking OW heartbeats
 * - Step names include campaign name for memoization isolation
 * - Campaign state tracked in public.campaign_runs (fire-and-forget DB writes)
 */

import { OpenWorkflow } from "openworkflow";
import { exec } from "node:child_process";
import { parse as parseYaml, stringify as stringifyYaml } from "yaml";

import {
  parseCampaignSpec,
  type CampaignSpec,
  type CampaignPhase,
  type CampaignWorker,
  type CampaignGateCheck,
} from "./campaign-spec-parser.js";

import {
  createDispatchStep,
  type DispatchWorkerInput,
  type DispatchWorkerOutput,
} from "../extensions/dispatch-worker-step.js";

import {
  createGateCheckStep,
  GateCheckError,
  type GateCheckInput,
} from "../extensions/gate-check-step.js";

import type { StepApi } from "../extensions/tracked-step.js";

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

interface CampaignRunnerInput {
  spec_path: string;
  campaign_dir: string;
  // camelCase aliases (PostgreSQL toCamel transform)
  specPath?: string;
  campaignDir?: string;
}

interface WorkerResult {
  id: string;
  status: string;
  elapsed_seconds: number;
  complete_yaml?: Record<string, unknown> | null;
}

interface PhaseResult {
  name: string;
  status: "completed" | "failed" | "skipped";
  worker_results: WorkerResult[];
  gate_passed: boolean | null;
}

interface CampaignRunnerOutput {
  campaign_name: string;
  status: "completed" | "partial" | "failed";
  phases_completed: number;
  phases_total: number;
  workers_completed: number;
  workers_total: number;
  workers_failed: number;
  phase_results: PhaseResult[];
  elapsed_seconds: number;
}

/** Internal type for tracking parallel dispatch state */
interface DispatchInfo {
  workerId: string;
  sessionName: string;
  completeFile: string;
  dispatchStatus: "dispatched" | "pre_completed" | "dispatch_failed";
  completeYaml?: Record<string, unknown>;
  error?: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LAPTOP_SSH = "matthewchapin@mac.lan";

const LAUNCHERS: Record<string, string> = {
  cpb: "$HOME/.claude/scripts/launchers/cpb.sh",
  cbb: "$HOME/.claude/scripts/launchers/cbb.sh",
  cmb: "$HOME/.claude/scripts/launchers/cmb.sh",
  clb: "$HOME/.claude/scripts/launchers/clb.sh",
  "local-glm": "$HOME/.claude/scripts/tools/local-task-dispatch.sh",
  "local-dispatch": "$HOME/.claude/scripts/tools/local-task-dispatch.sh",
};

/** Local launcher → Ollama model mapping. Keys must match LAUNCHERS. */
const LOCAL_LAUNCHER_MODELS: Record<string, string> = {
  "local-glm": "glm-4.7-flash",
  "local-dispatch": "glm-4.7-flash",
};

const DISPATCH_SCRIPT = "$HOME/.claude/scripts/dispatch-with-ritual.sh";

const SESSION_NAME_RE = /^[a-zA-Z0-9_-]+$/;

/** When true, run commands locally instead of via SSH */
const LOCAL_MODE = process.env.OW_LOCAL === "true";

/** SSH retry delays for critical operations: 5s, 15s, 45s */
const SSH_RETRY_DELAYS_MS = [5_000, 15_000, 45_000];

/** Polling interval for parallel watch step */
const WATCH_POLL_INTERVAL_MS = 10_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Dual-convention accessor for PostgreSQL camelCase-transformed inputs.
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
 * Async shell exec. Does NOT block the event loop.
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
          reject(
            new Error(`Command failed (code ${error.code ?? "?"}): ${msg}`),
          );
          return;
        }
        resolve(stdout.trim());
      },
    );
  });
}

/**
 * Execute a command on the laptop via SSH with retry + exponential backoff.
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
          `[campaign] SSH retry ${attempt + 1}/${SSH_RETRY_DELAYS_MS.length} for ${label} (waiting ${delay / 1000}s)`,
        );
        await sleep(delay);
      }
    }
  }
  throw lastError ?? new Error(`SSH failed: ${label}`);
}

/**
 * Single-attempt SSH exec for polling (no retry — outer loop handles recovery).
 */
async function sshPoll(cmd: string, timeoutMs: number): Promise<string> {
  if (LOCAL_MODE) {
    const escaped = cmd.replace(/'/g, "'\\''");
    return execAsync(`bash -c '${escaped}'`, timeoutMs);
  }
  const escaped = cmd.replace(/'/g, "'\\''");
  return execAsync(`ssh ${LAPTOP_SSH} '${escaped}'`, timeoutMs);
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Escape a string for use in SQL single-quoted literals.
 */
// Dual-convention accessor for PostgreSQL camelCase-transformed JSONB.
function getField(obj, snakeKey, fallback) {
  if (obj[snakeKey] !== undefined) return obj[snakeKey];
  const camelKey = snakeKey.replace(/_([a-z])/g, (_, l) => l.toUpperCase());
  if (obj[camelKey] !== undefined) return obj[camelKey];
  return fallback;
}

function sqlEsc(s: string): string {
  return s.replace(/'/g, "''");
}

/**
 * Fire-and-forget database update via local psql on Hearth.
 * Failures are logged but never block the workflow.
 */
function dbFireAndForget(sql: string): void {
  const dbName = process.env.DB_NAME || "hearth";
  exec(
    `/opt/homebrew/opt/postgresql@16/bin/psql -h localhost -U claude_daemon -d ${dbName} -c ${JSON.stringify(sql)}`,
    { timeout: 10_000 },
    (error, _stdout, stderr) => {
      if (error) {
        console.warn("[campaign] DB update failed:", error.message.slice(0, 400), stderr ? "stderr: " + stderr.slice(0, 300) : "");
      }
    },
  );
}

// ---------------------------------------------------------------------------
// Parallel Dispatch: dispatch all workers without waiting
// ---------------------------------------------------------------------------

/**
 * Dispatch multiple workers in a single OW step.
 * Creates tmux sessions and sends prompts but does NOT wait for COMPLETE files.
 * Returns dispatch info for each worker (pre_completed, dispatched, or dispatch_failed).
 */
async function dispatchAllWorkers(
  workers: CampaignWorker[],
  campaignName: string,
): Promise<DispatchInfo[]> {
  const results: DispatchInfo[] = [];

  for (const worker of workers) {
    const sessionName = worker.id;

    // Validate session name
    if (!SESSION_NAME_RE.test(sessionName)) {
      results.push({
        workerId: worker.id,
        sessionName,
        completeFile: getField(worker, "complete_file", ""),
        dispatchStatus: "dispatch_failed",
        error: `Invalid session name: "${sessionName}"`,
      });
      continue;
    }

    try {
      // Check if COMPLETE.yaml already exists (from a prior run)
      const preCheck = await sshPoll(
        `test -f "${getField(worker, "complete_file", "")}" && echo EXISTS || echo NO`,
        15_000,
      );
      if (preCheck.includes("EXISTS")) {
        console.log(
          `[campaign] Worker ${worker.id} already complete — reading COMPLETE.yaml`,
        );
        const content = await sshExec(
          `cat "${getField(worker, "complete_file", "")}"`,
          30_000,
          `read-pre-complete-${worker.id}`,
        );
        let parsed: Record<string, unknown>;
        try {
          parsed = parseYaml(content) as Record<string, unknown>;
        } catch {
          parsed = { raw_content: content };
        }
        results.push({
          workerId: worker.id,
          sessionName,
          completeFile: getField(worker, "complete_file", ""),
          dispatchStatus: "pre_completed",
          completeYaml: parsed,
        });
        continue;
      }

      // Check if tmux session already exists (from partial prior run)
      const sessionCheck = await sshExec(
        `tmux has-session -t "=${sessionName}" 2>/dev/null && echo EXISTS || echo OK`,
        15_000,
        `check-session-${worker.id}`,
      );
      if (sessionCheck.includes("EXISTS")) {
        console.log(
          `[campaign] Worker ${worker.id} session already exists — adding to watch list`,
        );
        results.push({
          workerId: worker.id,
          sessionName,
          completeFile: getField(worker, "complete_file", ""),
          dispatchStatus: "dispatched",
        });
        continue;
      }

      // Check prompt file exists
      await sshExec(
        `test -f "${worker.prompt}"`,
        15_000,
        `check-prompt-${worker.id}`,
      );

      // Dispatch: local vs ritual
      const launcherPath = LAUNCHERS[worker.launcher] ?? LAUNCHERS.cpb;
      const isLocalLauncher = worker.launcher in LOCAL_LAUNCHER_MODELS;

      console.log(
        `[campaign] Dispatching ${worker.id} (launcher=${worker.launcher}, local=${isLocalLauncher})`,
      );

      if (isLocalLauncher) {
        // Local model dispatch: runs synchronously, produces COMPLETE.yaml directly
        const model = LOCAL_LAUNCHER_MODELS[worker.launcher];
        const completeFile = getField(worker, "complete_file", "");
        const completeDir = completeFile.substring(0, completeFile.lastIndexOf("/"));
        await sshExec(
          `mkdir -p "${completeDir}" && ${launcherPath} --model ${model} --prompt-file "${worker.prompt}" --output-dir "/tmp/local-${sessionName}" --task-id "${sessionName}" && cp "/tmp/local-${sessionName}/COMPLETE.yaml" "${completeFile}"`,
          300_000, // 5 min for local model execution
          `local-dispatch-${worker.id}`,
        );
        console.log(`[campaign] Local dispatch complete: ${worker.id} (model=${model})`);

        // Read the COMPLETE.yaml that was just produced
        let parsed: Record<string, unknown> = {};
        try {
          const content = await sshExec(
            `cat "${completeFile}"`,
            30_000,
            `read-local-complete-${worker.id}`,
          );
          parsed = parseYaml(content) as Record<string, unknown>;
        } catch {
          parsed = { status: "completed", source: "local-dispatch" };
        }
        results.push({
          workerId: worker.id,
          sessionName,
          completeFile: getField(worker, "complete_file", ""),
          dispatchStatus: "pre_completed",
          completeYaml: parsed,
        });
      } else {
        // Claude Code dispatch via ritual script
        const owRunId = process.env.OW_RUN_ID ?? "ow-campaign";
        const contextFile = getField(worker, "context_file", "");
        const contextArg = contextFile ? ` --context-file "${contextFile}"` : "";

        await sshExec(
          `OW_RUN_ID=${owRunId} ${DISPATCH_SCRIPT} "${sessionName}" "${worker.prompt}" ${launcherPath}${contextArg}`,
          180_000, // 3 min for ~75s ritual + buffer
          `dispatch-${worker.id}`,
        );

        console.log(`[campaign] Dispatched: ${worker.id}`);
        results.push({
          workerId: worker.id,
          sessionName,
          completeFile: getField(worker, "complete_file", ""),
          dispatchStatus: "dispatched",
        });
      }
    } catch (error) {
      console.error(
        `[campaign] Dispatch failed for ${worker.id}: ${(error as Error).message.slice(0, 300)}`,
      );
      results.push({
        workerId: worker.id,
        sessionName,
        completeFile: getField(worker, "complete_file", ""),
        dispatchStatus: "dispatch_failed",
        error: (error as Error).message.slice(0, 500),
      });
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// Parallel Watch: poll for all COMPLETE files simultaneously
// ---------------------------------------------------------------------------

/**
 * Watch for COMPLETE.yaml files from multiple workers simultaneously.
 * Polls all pending files each cycle. Returns when all workers have
 * completed, timed out, or died.
 */
async function watchAllWorkers(
  dispatched: DispatchInfo[],
  workers: CampaignWorker[],
): Promise<WorkerResult[]> {
  const results: WorkerResult[] = [];
  const startTime = Date.now();

  // Pre-completed and failed workers go directly to results
  for (const d of dispatched) {
    if (d.dispatchStatus === "pre_completed") {
      results.push({
        id: d.workerId,
        status: "completed",
        elapsed_seconds: 0,
        complete_yaml: d.completeYaml ?? null,
      });
    } else if (d.dispatchStatus === "dispatch_failed") {
      results.push({
        id: d.workerId,
        status: "failed_to_start",
        elapsed_seconds: 0,
        complete_yaml: null,
      });
    }
  }

  // Workers still pending (dispatched, need watching)
  const pending = dispatched
    .filter((d) => d.dispatchStatus === "dispatched")
    .map((d) => ({ ...d }));

  // Build timeout map from worker specs
  const workerTimeouts = new Map<string, number>();
  for (const w of workers) {
    workerTimeouts.set(w.id, w.timeout * 1000);
  }

  // Max timeout across all workers (used as outer limit)
  const maxTimeout = Math.max(...workers.map((w) => w.timeout * 1000));

  console.log(
    `[campaign] Watching ${pending.length} workers (${results.length} pre-resolved)`,
  );

  while (pending.length > 0 && Date.now() - startTime < maxTimeout) {
    for (let i = pending.length - 1; i >= 0; i--) {
      const worker = pending[i];
      const workerTimeout = workerTimeouts.get(worker.workerId) ?? 3600_000;
      const elapsed = Date.now() - startTime;

      // Check per-worker timeout
      if (elapsed >= workerTimeout) {
        console.log(`[campaign] TIMEOUT: ${worker.workerId}`);
        results.push({
          id: worker.workerId,
          status: "timeout",
          elapsed_seconds: elapsed / 1000,
          complete_yaml: null,
        });
        pending.splice(i, 1);
        continue;
      }

      // Check COMPLETE.yaml
      try {
        const exists = await sshPoll(
          `test -f "${worker.completeFile}" && echo EXISTS || echo NO`,
          30_000,
        );
        if (exists.includes("EXISTS")) {
          const content = await sshExec(
            `cat "${worker.completeFile}"`,
            30_000,
            `read-complete-${worker.workerId}`,
          );
          let parsed: Record<string, unknown>;
          try {
            parsed = parseYaml(content) as Record<string, unknown>;
          } catch {
            parsed = { raw_content: content };
          }
          console.log(
            `[campaign] COMPLETE: ${worker.workerId} (${(elapsed / 1000).toFixed(0)}s)`,
          );
          results.push({
            id: worker.workerId,
            status: "completed",
            elapsed_seconds: elapsed / 1000,
            complete_yaml: parsed,
          });
          pending.splice(i, 1);
          continue;
        }
      } catch {
        // SSH error during poll — try next cycle
      }

      // Check for session death
      try {
        const alive = await sshPoll(
          `tmux has-session -t "=${worker.sessionName}" 2>/dev/null && echo ALIVE || echo DEAD`,
          15_000,
        );
        if (alive.includes("DEAD")) {
          // Session died — check once more for COMPLETE.yaml (may have been written just before death)
          try {
            const lastCheck = await sshPoll(
              `test -f "${worker.completeFile}" && echo EXISTS || echo NO`,
              15_000,
            );
            if (lastCheck.includes("EXISTS")) {
              const content = await sshExec(
                `cat "${worker.completeFile}"`,
                30_000,
                `read-complete-postdeath-${worker.workerId}`,
              );
              let parsed: Record<string, unknown>;
              try {
                parsed = parseYaml(content) as Record<string, unknown>;
              } catch {
                parsed = { raw_content: content };
              }
              console.log(
                `[campaign] COMPLETE (post-death): ${worker.workerId}`,
              );
              results.push({
                id: worker.workerId,
                status: "completed",
                elapsed_seconds: (Date.now() - startTime) / 1000,
                complete_yaml: parsed,
              });
            } else {
              console.log(
                `[campaign] DEATH: ${worker.workerId} — no COMPLETE.yaml`,
              );
              results.push({
                id: worker.workerId,
                status: "context_death",
                elapsed_seconds: (Date.now() - startTime) / 1000,
                complete_yaml: null,
              });
            }
          } catch {
            results.push({
              id: worker.workerId,
              status: "context_death",
              elapsed_seconds: (Date.now() - startTime) / 1000,
              complete_yaml: null,
            });
          }
          pending.splice(i, 1);
        }
      } catch {
        // SSH error checking session — continue polling
      }
    }

    if (pending.length > 0) {
      await sleep(WATCH_POLL_INTERVAL_MS);
    }
  }

  // Any still pending after max timeout
  for (const worker of pending) {
    console.log(
      `[campaign] TIMEOUT (max): ${worker.workerId}`,
    );
    results.push({
      id: worker.workerId,
      status: "timeout",
      elapsed_seconds: (Date.now() - startTime) / 1000,
      complete_yaml: null,
    });
  }

  return results;
}

// ---------------------------------------------------------------------------
// Phase Execution
// ---------------------------------------------------------------------------

/**
 * Execute a single campaign phase — dispatch workers and run gate checks.
 */
async function executePhase(
  step: StepApi,
  spec: CampaignSpec,
  phase: CampaignPhase,
  campaignName: string,
): Promise<PhaseResult> {
  const workerResults: WorkerResult[] = [];

  console.log(
    `[campaign] === Phase: ${phase.name} (${phase.sequential ? "sequential" : "parallel"}, ${phase.workers.length} workers) ===`,
  );

  if (phase.sequential) {
    // --- Sequential: dispatch each worker and wait ---
    for (const worker of phase.workers) {
      console.log(
        `[campaign] Dispatching sequential worker: ${worker.id}`,
      );

      const result = await createDispatchStep(
        step,
        {
          session_name: worker.id,
          prompt_path: worker.prompt,
          launcher: worker.launcher,
          complete_file: getField(worker, "complete_file", ""),
          context_file: getField(worker, "context_file", ""),
          timeout_seconds: worker.timeout,
          ritual: true,
          post_dispatch_verify: true,
        } as DispatchWorkerInput,
        `dispatch-${campaignName}-${worker.id}`,
      );

      workerResults.push({
        id: worker.id,
        status: result.status,
        elapsed_seconds: result.elapsed_seconds,
        complete_yaml: result.complete_yaml,
      });

      // For sequential phases, stop on first failure
      if (result.status !== "completed") {
        console.log(
          `[campaign] Worker ${worker.id} did not complete (status=${result.status}). Stopping phase.`,
        );
        return {
          name: phase.name,
          status: "failed",
          worker_results: workerResults,
          gate_passed: null,
        };
      }
    }
  } else {
    // --- Parallel: dispatch all workers, then watch for all COMPLETE files ---

    // Step: dispatch all workers (creates sessions, sends prompts, no waiting)
    const dispatched = await step.run(
      { name: `dispatch-all-${campaignName}-${phase.name}` },
      () => dispatchAllWorkers(phase.workers, campaignName),
    );

    // Step: watch for all COMPLETE files
    const watchResults = await step.run(
      { name: `watch-all-${campaignName}-${phase.name}` },
      () => watchAllWorkers(dispatched, phase.workers),
    );

    workerResults.push(...watchResults);
  }

  // --- Gate check (only if all workers completed) ---
  const allCompleted = workerResults.every((w) => w.status === "completed");
  let gatePassed: boolean | null = null;

  if (phase.gate && phase.gate.length > 0) {
    if (!allCompleted) {
      console.log(
        `[campaign] Skipping gate for phase ${phase.name} — not all workers completed`,
      );
      gatePassed = null;
    } else {
      try {
        await createGateCheckStep(step, {
          gate_name: `${campaignName}-${phase.name}`,
          checks: phase.gate.map((c) => ({
            name: c.name,
            command: c.command,
            expect_exit_0: true,
            run_on: getField(c, "run_on", "laptop"),
          })),
          fail_fast: true,
        } as GateCheckInput);

        gatePassed = true;
        console.log(`[campaign] Gate PASSED: ${phase.name}`);
      } catch (error) {
        if (error instanceof GateCheckError) {
          console.log(
            `[campaign] Gate FAILED: ${phase.name} — ${error.message.slice(0, 300)}`,
          );
          gatePassed = false;
        } else {
          // Unexpected error — don't re-throw (spec says catch, don't let OW retry)
          console.error(
            `[campaign] Gate error: ${phase.name} — ${(error as Error).message.slice(0, 300)}`,
          );
          gatePassed = false;
        }
      }
    }
  }

  const phaseStatus =
    allCompleted && gatePassed !== false ? "completed" : "failed";

  console.log(
    `[campaign] Phase ${phase.name}: ${phaseStatus} (${workerResults.length} workers, gate=${gatePassed})`,
  );

  return {
    name: phase.name,
    status: phaseStatus,
    worker_results: workerResults,
    gate_passed: gatePassed,
  };
}

// ---------------------------------------------------------------------------
// Main Workflow Export
// ---------------------------------------------------------------------------

export function createCampaignRunnerWorkflow(ow: OpenWorkflow) {
  return ow.defineWorkflow<CampaignRunnerInput, CampaignRunnerOutput>(
    { name: "campaign-runner" },
    async ({ input, step }) => {
      const startTime = Date.now();

      // --- Resolve inputs (camelCase fallback) ---
      const specPath =
        get<string>(input as Record<string, unknown>, "spec_path") ?? "";
      const campaignDir =
        get<string>(input as Record<string, unknown>, "campaign_dir") ?? "";

      if (!specPath || !campaignDir) {
        throw new Error(
          `Missing required inputs: spec_path="${specPath}", campaign_dir="${campaignDir}"`,
        );
      }

      // --- Step 1: Read and parse campaign spec ---
      const spec = await step.run({ name: "read-spec" }, async () => {
        console.log(`[campaign] Reading spec from: ${specPath}`);
        const content = await sshExec(
          `cat "${specPath}"`,
          30_000,
          "read-spec",
        );
        return parseCampaignSpec(content, campaignDir);
      });

      const campaignName = spec.campaign.name;
      const totalWorkers = spec.phases.reduce(
        (sum, p) => sum + p.workers.length,
        0,
      );

      console.log(
        `[campaign] Starting campaign: ${campaignName} (${spec.phases.length} phases, ${totalWorkers} workers)`,
      );

      // --- Insert campaign run record (fire-and-forget) ---
      const campaignRunId = `${campaignName}-${Date.now()}`;
      dbFireAndForget(
        `INSERT INTO public.campaign_runs (id, campaign_name, spec_path, started_at, status, worker_count, phases_completed, phases_total, workflow_run_id) ` +
        `VALUES ('${sqlEsc(campaignRunId)}', '${sqlEsc(campaignName)}', '${sqlEsc(specPath)}', NOW(), 'running', ${totalWorkers}, 0, ${spec.phases.length}, '${sqlEsc(campaignRunId)}') ` +
        `ON CONFLICT DO NOTHING`,
      );

      // --- Step 2: Preflight checks ---
      if (spec.preflight.length > 0) {
        console.log(
          `[campaign] Running ${spec.preflight.length} preflight checks`,
        );
        await createGateCheckStep(step, {
          gate_name: `${campaignName}-preflight`,
          checks: spec.preflight.map((c) => ({
            name: c.name,
            command: c.command,
            expect_exit_0: true,
            run_on: getField(c, "run_on", "laptop"),
          })),
          fail_fast: true,
        } as GateCheckInput);
        console.log("[campaign] Preflight passed");
      }

      // --- Step 3: Execute phases in order ---
      const phaseResults: PhaseResult[] = [];
      let phasesCompleted = 0;

      for (const phase of spec.phases) {
        const phaseResult = await executePhase(
          step,
          spec,
          phase,
          campaignName,
        );
        phaseResults.push(phaseResult);

        if (phaseResult.status === "completed") {
          phasesCompleted++;
        }

        // Update DB with progress
        dbFireAndForget(
          `UPDATE public.campaign_runs SET phases_completed = ${phasesCompleted}, current_phase = '${sqlEsc(phase.name)}' ` +
          `WHERE id = '${sqlEsc(campaignRunId)}'`,
        );

        // If phase failed, stop the campaign
        if (phaseResult.status === "failed") {
          console.log(
            `[campaign] Phase ${phase.name} failed — stopping campaign`,
          );
          break;
        }
      }

      // --- Step 4: Aggregate results ---
      const workersCompleted = phaseResults.reduce(
        (sum, p) =>
          sum + p.worker_results.filter((w) => w.status === "completed").length,
        0,
      );
      const workersFailed = phaseResults.reduce(
        (sum, p) =>
          sum + p.worker_results.filter((w) => w.status !== "completed").length,
        0,
      );

      const campaignStatus: "completed" | "partial" | "failed" =
        phasesCompleted === spec.phases.length
          ? "completed"
          : phasesCompleted > 0
            ? "partial"
            : "failed";

      const elapsedSeconds = (Date.now() - startTime) / 1000;

      const result: CampaignRunnerOutput = {
        campaign_name: campaignName,
        status: campaignStatus,
        phases_completed: phasesCompleted,
        phases_total: spec.phases.length,
        workers_completed: workersCompleted,
        workers_total: totalWorkers,
        workers_failed: workersFailed,
        phase_results: phaseResults,
        elapsed_seconds: elapsedSeconds,
      };

      // --- Step 5: Write campaign COMPLETE.yaml ---
      await step.run(
        { name: `campaign-complete-${campaignName}` },
        async () => {
          const completeData = {
            campaign_name: campaignName,
            status: campaignStatus,
            completed_at: new Date().toISOString(),
            phases_completed: phasesCompleted,
            phases_total: spec.phases.length,
            workers_completed: workersCompleted,
            workers_total: totalWorkers,
            workers_failed: workersFailed,
            elapsed_seconds: Math.round(elapsedSeconds),
            phase_results: phaseResults.map((p) => ({
              name: p.name,
              status: p.status,
              gate_passed: p.gate_passed,
              workers: p.worker_results.map((w) => ({
                id: w.id,
                status: w.status,
                elapsed_seconds: Math.round(w.elapsed_seconds || 0),
              })),
            })),
          };

          const yamlString = stringifyYaml(completeData);
          const b64 = Buffer.from(yamlString).toString("base64");

          await sshExec(
            `printf "%s" "${b64}" | base64 -d > "${campaignDir}/campaign-COMPLETE.yaml"`,
            30_000,
            "write-campaign-complete",
          );

          console.log(
            `[campaign] Campaign COMPLETE.yaml written: ${campaignStatus}`,
          );
          return completeData;
        },
      );

      // --- Final DB update ---
      dbFireAndForget(
        `UPDATE public.campaign_runs SET status = '${sqlEsc(campaignStatus)}', completed_at = NOW(), phases_completed = ${phasesCompleted} ` +
        `WHERE id = '${sqlEsc(campaignRunId)}'`,
      );

      console.log(
        `[campaign] Campaign ${campaignName} finished: ${campaignStatus} (${Math.round(elapsedSeconds)}s)`,
      );

      return result;
    },
  );
}
