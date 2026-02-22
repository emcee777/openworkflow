/**
 * Campaign CLI Commands
 *
 * Extends the ow CLI with campaign-specific subcommands:
 * start, resume, status, list, cancel.
 *
 * Worker 2B — OW-Everywhere campaign.
 */

import type { BackendPostgres } from "openworkflow/postgres";
import type { WorkflowRun } from "openworkflow/core/workflow.js";
import type { StepAttempt } from "openworkflow/core/step.js";
import { parse as parseYaml } from "yaml";
import { readFileSync, existsSync } from "fs";
import { dirname } from "path";
import { exec } from "child_process";
import { getDatabaseUrl, config } from "../config.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LAPTOP_SSH = "matthewchapin@mac.lan";
const CAMPAIGN_WORKFLOW_NAME = "campaign-runner";

/** When true, run commands locally instead of via SSH */
const LOCAL_MODE = process.env.OW_LOCAL === "true";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function execAsync(cmd: string, timeoutMs = 30_000): Promise<string> {
  return new Promise((resolve, reject) => {
    exec(cmd, { timeout: timeoutMs, maxBuffer: 10 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error) {
        reject(new Error((stderr || error.message || "unknown error").slice(0, 2000)));
        return;
      }
      resolve(stdout.trim());
    });
  });
}

async function sshExec(cmd: string, timeoutMs = 30_000): Promise<string> {
  if (LOCAL_MODE) {
    // Use single quotes to prevent the outer sh from expanding $variables
    const escaped = cmd.replace(/'/g, "'\\''");
    return execAsync(`bash -c '${escaped}'`, timeoutMs);
  }
  const escaped = cmd.replace(/'/g, "'\\''");
  return execAsync(`ssh ${LAPTOP_SSH} '${escaped}'`, timeoutMs);
}

function psqlExec(sql: string, timeoutMs = 10_000): Promise<string> {
  const dbUrl = getDatabaseUrl();
  const escaped = sql.replace(/'/g, "'\\''");
  return execAsync(
    `psql '${dbUrl}' -t -A -c '${escaped}'`,
    timeoutMs,
  );
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}

function formatTimestamp(date: Date): string {
  return date.toISOString().replace("T", " ").slice(0, 19);
}

// ---------------------------------------------------------------------------
// Spec reading (for start command display)
// ---------------------------------------------------------------------------

interface SpecSummary {
  name: string;
  phaseCount: number;
  workerCount: number;
  phases: Array<{ name: string; workerCount: number; sequential: boolean }>;
}

async function readSpecSummary(specPath: string): Promise<SpecSummary> {
  let content: string;

  if (LOCAL_MODE && existsSync(specPath)) {
    // Local mode — read directly from filesystem
    content = readFileSync(specPath, "utf-8");
  } else if (LOCAL_MODE) {
    // Local mode but file might need shell expansion (e.g., $HOME in path)
    content = await sshExec(`cat "${specPath}"`);
  } else if (specPath.startsWith("/Users/matthewchapin")) {
    // Spec is on the laptop — read via SSH
    content = await sshExec(`cat "${specPath}"`);
  } else if (existsSync(specPath)) {
    // Spec is local (on Hearth)
    content = readFileSync(specPath, "utf-8");
  } else {
    throw new Error(`Spec file not found: ${specPath}`);
  }

  const raw = parseYaml(content) as Record<string, unknown>;
  const campaign = raw.campaign as Record<string, unknown>;
  const phases = raw.phases as Array<Record<string, unknown>>;

  const phaseSummaries = phases.map((phase) => {
    const workers = phase.workers as Array<unknown>;
    return {
      name: String(phase.name),
      workerCount: workers.length,
      sequential: phase.sequential !== false,
    };
  });

  return {
    name: String(campaign.name),
    phaseCount: phases.length,
    workerCount: phaseSummaries.reduce((sum, p) => sum + p.workerCount, 0),
    phases: phaseSummaries,
  };
}

// ---------------------------------------------------------------------------
// campaign start
// ---------------------------------------------------------------------------

async function campaignStart(
  backend: BackendPostgres,
  args: string[],
): Promise<void> {
  const specPath = args[0];
  if (!specPath) {
    console.error("Usage: ow campaign start <spec.yaml> [--campaign-dir <dir>]");
    process.exit(1);
  }

  // Parse --campaign-dir flag
  const dirFlagIndex = args.indexOf("--campaign-dir");
  const campaignDir =
    dirFlagIndex > -1 && args[dirFlagIndex + 1]
      ? args[dirFlagIndex + 1]
      : dirname(specPath);

  // Read spec for display
  let summary: SpecSummary;
  try {
    summary = await readSpecSummary(specPath);
  } catch (error) {
    console.error(`Failed to read spec: ${(error as Error).message}`);
    process.exit(1);
  }

  // Trigger campaign-runner workflow
  const input = { spec_path: specPath, campaign_dir: campaignDir };

  // Use the backend to create a workflow run directly
  const workflowRun = await backend.createWorkflowRun({
    workflowName: CAMPAIGN_WORKFLOW_NAME,
    version: null,
    idempotencyKey: null,
    config: {},
    context: null,
    input: input as unknown as import("openworkflow/core/json.js").JsonValue,
    availableAt: null,
    deadlineAt: null,
  });

  console.log(`Campaign: ${summary.name}`);
  console.log(`Run ID:   ${workflowRun.id}`);
  console.log(`Phases:   ${summary.phaseCount}`);
  console.log(`Workers:  ${summary.workerCount}`);
  console.log("");
  for (const phase of summary.phases) {
    const mode = phase.sequential ? "sequential" : "parallel";
    console.log(`  ${phase.name}: ${phase.workerCount} workers (${mode})`);
  }
  console.log("");
  console.log(`Resume with: ow campaign resume ${workflowRun.id}`);
  console.log(`Status with: ow campaign status ${workflowRun.id}`);
}

// ---------------------------------------------------------------------------
// campaign resume
// ---------------------------------------------------------------------------

async function campaignResume(
  backend: BackendPostgres,
  args: string[],
): Promise<void> {
  const runId = args[0];
  if (!runId) {
    console.error("Usage: ow campaign resume <run-id>");
    process.exit(1);
  }

  const run = await backend.getWorkflowRun({ workflowRunId: runId });
  if (!run) {
    console.error(`Run not found: ${runId}`);
    process.exit(1);
  }

  if (run.status === "completed" || run.status === "succeeded") {
    console.log("Campaign already completed.");
    return;
  }

  if (run.status === "canceled") {
    console.log("Campaign was canceled — use `ow campaign start` for a new run.");
    return;
  }

  if (run.status === "pending" || run.status === "running") {
    console.log(`Campaign is already ${run.status} — daemon will pick it up.`);
    return;
  }

  // Status is "failed" or "sleeping" — reset to pending
  // OW step memoization preserves completed steps; execution resumes
  // from the first uncompleted step.
  //
  // Validate runId is a UUID to prevent SQL injection.
  // (It's already verified via getWorkflowRun, but defense in depth.)
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (!UUID_RE.test(runId)) {
    console.error("Invalid run ID format.");
    process.exit(1);
  }
  const namespace = config.database.namespace;
  await psqlExec(
    `UPDATE openworkflow.workflow_runs ` +
    `SET status = 'pending', error = NULL, worker_id = NULL, ` +
    `available_at = NOW(), updated_at = NOW() ` +
    `WHERE namespace_id = '${namespace}' AND id = '${runId}'`,
  );

  // Query completed steps to show what will be skipped
  const steps = await listAllStepAttempts(backend, runId);
  const completed = steps.filter(
    (s) => s.status === "completed" || s.status === "succeeded",
  );

  console.log(`Resumed: ${runId}`);
  console.log(`Status reset to: pending`);
  if (completed.length > 0) {
    console.log(`\nSkipping ${completed.length} completed steps:`);
    for (const step of completed) {
      console.log(`  ✓ ${step.stepName}`);
    }
  }
  console.log("\nThe OW daemon will pick this up within 1s.");
}

// ---------------------------------------------------------------------------
// campaign status
// ---------------------------------------------------------------------------

interface StepDisplay {
  name: string;
  status: string;
  elapsed: string;
  icon: string;
}

function stepIcon(status: string): string {
  switch (status) {
    case "completed":
    case "succeeded":
      return "✓";
    case "running":
      return "◷";
    case "failed":
      return "✗";
    default:
      return "?";
  }
}

function stepElapsed(step: StepAttempt): string {
  if (!step.startedAt) return "";
  const end = step.finishedAt ?? new Date();
  const elapsed = (end.getTime() - step.startedAt.getTime()) / 1000;
  return formatDuration(elapsed);
}

async function listAllStepAttempts(
  backend: BackendPostgres,
  runId: string,
): Promise<StepAttempt[]> {
  const allSteps: StepAttempt[] = [];
  let cursor: string | undefined;
  do {
    const response = await backend.listStepAttempts({
      workflowRunId: runId,
      ...(cursor ? { after: cursor } : {}),
      limit: 200,
    });
    allSteps.push(...response.data);
    cursor = response.pagination.next ?? undefined;
  } while (cursor);
  return allSteps;
}

async function campaignStatus(
  backend: BackendPostgres,
  args: string[],
): Promise<void> {
  const runId = args[0];
  if (!runId) {
    console.error("Usage: ow campaign status <run-id>");
    process.exit(1);
  }

  const run = await backend.getWorkflowRun({ workflowRunId: runId });
  if (!run) {
    console.error(`Run not found: ${runId}`);
    process.exit(1);
  }

  // Extract campaign name from input
  const input = run.input as Record<string, unknown> | null;
  const specPath = input?.spec_path ?? input?.specPath ?? "unknown";

  // Get all step attempts
  const steps = await listAllStepAttempts(backend, runId);

  // Calculate elapsed time
  const startTime = run.startedAt ?? run.createdAt;
  const endTime = run.finishedAt ?? new Date();
  const elapsedSec = (endTime.getTime() - startTime.getTime()) / 1000;

  // Identify campaign name from steps
  // read-spec step output contains the parsed spec with campaign name
  const readSpecStep = steps.find((s) => s.stepName === "read-spec");
  let campaignName = "unknown";
  if (readSpecStep?.output && typeof readSpecStep.output === "object") {
    const specOutput = readSpecStep.output as Record<string, unknown>;
    const campaign = specOutput.campaign as Record<string, unknown> | undefined;
    campaignName = String(campaign?.name ?? specOutput.campaignName ?? "unknown");
  }

  // Group steps for display
  // Step naming conventions from campaign-runner.ts:
  //   read-spec
  //   gate-${campaignName}-preflight
  //   dispatch-${campaignName}-${workerId}          (sequential)
  //   dispatch-all-${campaignName}-${phaseName}     (parallel dispatch)
  //   watch-all-${campaignName}-${phaseName}        (parallel watch)
  //   gate-${campaignName}-${phaseName}             (phase gate)
  //   campaign-complete-${campaignName}             (final write)

  const prefix = campaignName !== "unknown" ? campaignName : "";

  // Header
  console.log(`Campaign: ${campaignName} (run: ${runId.slice(0, 8)})`);
  console.log(`Status: ${run.status}`);
  console.log("");

  // Preflight
  const preflightStep = steps.find(
    (s) => s.stepName === `gate-${prefix}-preflight`,
  );
  if (preflightStep) {
    const icon = stepIcon(preflightStep.status);
    const output = preflightStep.output as Record<string, unknown> | null;
    const total = output?.total_count ?? output?.totalCount ?? "?";
    const passed = output?.passed ? "PASSED" : "FAILED";
    console.log(`Preflight: ${icon} ${passed} (${total} checks)`);
    console.log("");
  }

  // Organize steps into phases
  // Detect phases by looking for dispatch-* or dispatch-all-* step names
  const phaseSteps = new Map<string, StepAttempt[]>();
  const miscSteps: StepAttempt[] = [];

  for (const step of steps) {
    const name = step.stepName;

    if (name === "read-spec" || name === `campaign-complete-${prefix}`) {
      miscSteps.push(step);
      continue;
    }
    if (name === `gate-${prefix}-preflight`) {
      continue; // Already displayed above
    }

    // Try to extract phase or worker info from step name
    // Sequential dispatch: dispatch-${campaign}-${workerId}
    // Parallel dispatch: dispatch-all-${campaign}-${phaseName}
    // Parallel watch: watch-all-${campaign}-${phaseName}
    // Gate: gate-${campaign}-${phaseName}

    let phaseName: string | null = null;

    if (name.startsWith(`dispatch-all-${prefix}-`)) {
      phaseName = name.slice(`dispatch-all-${prefix}-`.length);
    } else if (name.startsWith(`watch-all-${prefix}-`)) {
      // Legacy monolithic watch step (pre-Flame 8466)
      phaseName = name.slice(`watch-all-${prefix}-`.length);
    } else if (name.startsWith(`watch-${prefix}-`)) {
      // Per-worker watch step (Flame 8466+) — group as individual worker
      const workerId = name.slice(`watch-${prefix}-`.length);
      phaseName = `worker:${workerId}`;
    } else if (name.startsWith(`gate-${prefix}-`)) {
      phaseName = name.slice(`gate-${prefix}-`.length);
    } else if (name.startsWith(`dispatch-${prefix}-`)) {
      // Sequential worker — the worker ID is the remainder
      // We don't know which phase it belongs to without the spec,
      // so group by "workers" as a fallback
      const workerId = name.slice(`dispatch-${prefix}-`.length);
      phaseName = `worker:${workerId}`;
    }

    if (phaseName) {
      if (!phaseSteps.has(phaseName)) {
        phaseSteps.set(phaseName, []);
      }
      phaseSteps.get(phaseName)!.push(step);
    } else {
      miscSteps.push(step);
    }
  }

  // Display phases
  // First, collect individual worker entries for sequential phases
  const workerEntries = new Map<string, StepAttempt>();
  const phaseEntries = new Map<string, StepAttempt[]>();

  for (const [key, phaseStepList] of phaseSteps) {
    if (key.startsWith("worker:")) {
      const workerId = key.slice("worker:".length);
      workerEntries.set(workerId, phaseStepList[0]!);
    } else {
      phaseEntries.set(key, phaseStepList);
    }
  }

  // Display parallel phases
  for (const [phaseName, phaseStepList] of phaseEntries) {
    console.log(`${phaseName}:`);
    for (const step of phaseStepList) {
      const icon = stepIcon(step.status);
      const elapsed = stepElapsed(step);
      const elapsedStr = elapsed ? `, ${elapsed}` : "";
      const shortName = step.stepName
        .replace(`${prefix}-`, "")
        .replace(`-${phaseName}`, "");
      console.log(`  ${icon} ${shortName} (${step.status}${elapsedStr})`);

      // For watch-all steps, try to show per-worker results from output
      if (
        step.stepName.startsWith("watch-all-") &&
        step.output &&
        Array.isArray(step.output)
      ) {
        for (const result of step.output as Array<Record<string, unknown>>) {
          const wIcon = result.status === "completed" ? "✓" : "✗";
          const wElapsed = typeof result.elapsed_seconds === "number"
            ? formatDuration(result.elapsed_seconds as number)
            : typeof result.elapsedSeconds === "number"
              ? formatDuration(result.elapsedSeconds as number)
              : "";
          const wId = result.id ?? result.workerId ?? "unknown";
          console.log(`    ${wIcon} ${wId} (${result.status}, ${wElapsed})`);
        }
      }
    }

    // Show gate for this phase if it exists in the step list
    const gateStep = steps.find(
      (s) => s.stepName === `gate-${prefix}-${phaseName}`,
    );
    if (gateStep) {
      const icon = stepIcon(gateStep.status);
      const output = gateStep.output as Record<string, unknown> | null;
      const passed = output?.passed ? "passed" : "failed";
      console.log(`  ${icon} gate (${passed})`);
    }
    console.log("");
  }

  // Display sequential workers
  if (workerEntries.size > 0) {
    console.log("Workers:");
    for (const [workerId, step] of workerEntries) {
      const icon = stepIcon(step.status);
      const elapsed = stepElapsed(step);
      const elapsedStr = elapsed ? `, ${elapsed}` : "";
      console.log(`  ${icon} ${workerId} (${step.status}${elapsedStr})`);
    }
    console.log("");
  }

  // Summary line
  const completedSteps = steps.filter(
    (s) => s.status === "completed" || s.status === "succeeded",
  ).length;
  const failedSteps = steps.filter((s) => s.status === "failed").length;
  const runningSteps = steps.filter((s) => s.status === "running").length;

  console.log(
    `Elapsed: ${formatDuration(elapsedSec)} | ` +
    `Steps: ${completedSteps}/${steps.length} complete` +
    (failedSteps > 0 ? `, ${failedSteps} failed` : "") +
    (runningSteps > 0 ? `, ${runningSteps} running` : ""),
  );

  // Show error if failed
  if (run.status === "failed" && run.error) {
    console.log("");
    const errMsg =
      typeof run.error === "object" && run.error !== null
        ? (run.error as Record<string, unknown>).message ?? JSON.stringify(run.error)
        : String(run.error);
    console.error(`Error: ${String(errMsg).slice(0, 500)}`);
  }
}

// ---------------------------------------------------------------------------
// campaign list
// ---------------------------------------------------------------------------

async function campaignList(backend: BackendPostgres): Promise<void> {
  // List workflow runs, filter client-side for campaign-runner
  // Fetch more than 10 to account for non-campaign runs in the list
  const response = await backend.listWorkflowRuns({ limit: 100 });
  const campaignRuns = response.data
    .filter((r: WorkflowRun) => r.workflowName === CAMPAIGN_WORKFLOW_NAME)
    .slice(0, 10);

  if (campaignRuns.length === 0) {
    console.log("No campaign runs found.");
    return;
  }

  for (const run of campaignRuns) {
    const input = run.input as Record<string, unknown> | null;
    const specPath = String(input?.spec_path ?? input?.specPath ?? "");
    // Extract campaign name from spec path (e.g., /path/to/campaign-name/spec.yaml → campaign-name)
    const pathParts = specPath.split("/");
    const campaignName =
      pathParts.length >= 2
        ? pathParts[pathParts.length - 2]
        : specPath || "unknown";

    console.log(
      `${run.id}  ${(campaignName ?? "unknown").slice(0, 25).padEnd(25)}  ` +
      `${run.status.padEnd(12)}  ${formatTimestamp(run.createdAt)}`,
    );
  }
}

// ---------------------------------------------------------------------------
// campaign cancel
// ---------------------------------------------------------------------------

async function campaignCancel(
  backend: BackendPostgres,
  args: string[],
): Promise<void> {
  const runId = args[0];
  if (!runId) {
    console.error("Usage: ow campaign cancel <run-id>");
    process.exit(1);
  }

  const run = await backend.getWorkflowRun({ workflowRunId: runId });
  if (!run) {
    console.error(`Run not found: ${runId}`);
    process.exit(1);
  }

  try {
    await backend.cancelWorkflowRun({ workflowRunId: runId });
    console.log(`Canceled: ${runId}`);
  } catch (error) {
    console.error(`Cancel failed: ${(error as Error).message}`);
    process.exit(1);
  }

  // Check for associated tmux sessions on the laptop
  // Campaign input contains spec_path — extract campaign name for session prefix
  const input = run.input as Record<string, unknown> | null;
  if (input) {
    console.log("\nNote: Worker tmux sessions may still be running on the laptop.");
    console.log("Kill them manually if needed: tmux kill-session -t <session-name>");
  }
}

// ---------------------------------------------------------------------------
// Help
// ---------------------------------------------------------------------------

function showHelp(): void {
  console.log("Usage: ow campaign <command> [options]");
  console.log("");
  console.log("Commands:");
  console.log("  start <spec.yaml> [--campaign-dir <dir>]  Start a campaign");
  console.log("  resume <run-id>                           Resume a failed campaign");
  console.log("  status <run-id>                           Show campaign status");
  console.log("  list                                      List recent campaigns");
  console.log("  cancel <run-id>                           Cancel a running campaign");
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

export async function runCampaignCommand(
  backend: BackendPostgres,
  args: string[],
): Promise<void> {
  const subcommand = args[0];
  const rest = args.slice(1);

  switch (subcommand) {
    case "start":
      return campaignStart(backend, rest);
    case "resume":
      return campaignResume(backend, rest);
    case "status":
      return campaignStatus(backend, rest);
    case "list":
      return campaignList(backend);
    case "cancel":
      return campaignCancel(backend, rest);
    default:
      showHelp();
  }
}
