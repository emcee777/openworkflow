/**
 * Campaign Spec Parser
 *
 * Parses campaign specification YAML files into a typed CampaignSpec structure.
 * Resolves relative paths against the campaign directory and validates required fields.
 *
 * Used by campaign-runner.ts to interpret campaign specs for OW workflow execution.
 */

import { parse as parseYaml } from "yaml";
import { join, isAbsolute } from "path";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface CampaignWorker {
  id: string;
  prompt: string;           // Absolute path (resolved against campaignDir)
  launcher: string;         // "cpb" | "cbb" | "cmb" | "clb"
  timeout: number;          // seconds
  complete_file: string;    // Absolute path (resolved against campaignDir)
  context_file?: string;    // Optional path to supplementary context file
}

export interface CampaignGateCheck {
  name: string;
  command: string;
  run_on: "laptop" | "hearth";
}

export interface CampaignPhase {
  name: string;
  sequential: boolean;
  depends_on?: string[];
  workers: CampaignWorker[];
  gate?: CampaignGateCheck[];
}

export interface CampaignSpec {
  campaign: {
    name: string;
    version: number;
  };
  preflight: CampaignGateCheck[];
  phases: CampaignPhase[];
  success_criteria?: string[];
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

const VALID_NAME_RE = /^[a-zA-Z0-9_-]+$/;
const VALID_LAUNCHERS = ["cpb", "cbb", "cmb", "clb", "c5b", "local-glm", "local-dispatch"];

/** Minimum timeout for any worker (30 minutes). Prevents false DEAD declarations
 *  from specs that set dangerously low timeouts (e.g., 749s caused layer-completion failure). */
const MIN_TIMEOUT_SECONDS = 1800;
const DEFAULT_TIMEOUT_SECONDS = 3600;

export class ParseError extends Error {
  constructor(message: string) {
    super(`Campaign spec parse error: ${message}`);
    this.name = "ParseError";
  }
}

function validateRequired(
  obj: Record<string, unknown>,
  field: string,
  context: string,
): void {
  if (obj[field] === undefined || obj[field] === null || obj[field] === "") {
    throw new ParseError(`Missing required field "${field}" in ${context}`);
  }
}

// ---------------------------------------------------------------------------
// Path Resolution
// ---------------------------------------------------------------------------

/**
 * Resolve a path relative to the campaign directory.
 * Absolute paths are returned as-is.
 */
function resolvePath(path: string, campaignDir: string): string {
  if (isAbsolute(path)) return path;
  return join(campaignDir, path);
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/**
 * Parse a campaign specification YAML string into a typed CampaignSpec.
 *
 * @param yamlContent - Raw YAML string
 * @param campaignDir - Absolute path to campaign directory (for resolving relative paths)
 * @returns Validated and path-resolved CampaignSpec
 * @throws ParseError if required fields are missing or invalid
 */
export function parseCampaignSpec(
  yamlContent: string,
  campaignDir: string,
): CampaignSpec {
  const raw = parseYaml(yamlContent) as Record<string, unknown>;
  if (!raw || typeof raw !== "object") {
    throw new ParseError("YAML content is not a valid object");
  }

  // --- Campaign metadata ---
  const campaign = raw.campaign as Record<string, unknown> | undefined;
  if (!campaign || typeof campaign !== "object") {
    throw new ParseError("Missing required 'campaign' section");
  }
  validateRequired(campaign, "name", "campaign");

  const campaignName = String(campaign.name);
  if (!VALID_NAME_RE.test(campaignName)) {
    throw new ParseError(
      `Campaign name "${campaignName}" must match ${VALID_NAME_RE.source}`,
    );
  }
  const campaignVersion =
    typeof campaign.version === "number" ? campaign.version : 1;

  // --- Preflight checks ---
  const rawPreflight =
    (raw.preflight as Array<Record<string, unknown>>) ?? [];
  const preflight: CampaignGateCheck[] = rawPreflight.map((check, i) => {
    validateRequired(check, "name", `preflight[${i}]`);
    validateRequired(check, "command", `preflight[${i}]`);
    return {
      name: String(check.name),
      command: String(check.command),
      run_on: (check.run_on as "laptop" | "hearth") ?? "laptop",
    };
  });

  // --- Phases ---
  const rawPhases = raw.phases as Array<Record<string, unknown>> | undefined;
  if (!rawPhases || !Array.isArray(rawPhases) || rawPhases.length === 0) {
    throw new ParseError("Must have at least one phase");
  }

  const phases: CampaignPhase[] = rawPhases.map((phase, pi) => {
    validateRequired(phase, "name", `phases[${pi}]`);
    const phaseName = String(phase.name);

    const rawWorkers =
      phase.workers as Array<Record<string, unknown>> | undefined;
    if (!rawWorkers || !Array.isArray(rawWorkers) || rawWorkers.length === 0) {
      throw new ParseError(
        `Phase "${phaseName}" must have at least one worker`,
      );
    }

    const workers: CampaignWorker[] = rawWorkers.map((worker, wi) => {
      validateRequired(worker, "id", `phases[${pi}].workers[${wi}]`);
      validateRequired(worker, "prompt", `phases[${pi}].workers[${wi}]`);
      validateRequired(worker, "launcher", `phases[${pi}].workers[${wi}]`);

      const workerId = String(worker.id);
      if (!VALID_NAME_RE.test(workerId)) {
        throw new ParseError(
          `Worker id "${workerId}" must match ${VALID_NAME_RE.source}`,
        );
      }

      const launcher = String(worker.launcher);
      if (!VALID_LAUNCHERS.includes(launcher)) {
        throw new ParseError(
          `Worker "${workerId}" has invalid launcher "${launcher}". Valid: ${VALID_LAUNCHERS.join(", ")}`,
        );
      }

      const rawTimeout =
          typeof worker.timeout === "number" ? worker.timeout : DEFAULT_TIMEOUT_SECONDS;
      const effectiveTimeout = Math.max(rawTimeout, MIN_TIMEOUT_SECONDS);
      if (rawTimeout < MIN_TIMEOUT_SECONDS) {
        console.warn(
          `[campaign-spec] Worker "${workerId}" timeout ${rawTimeout}s below minimum — raised to ${MIN_TIMEOUT_SECONDS}s`,
        );
      }

      return {
        id: workerId,
        prompt: resolvePath(String(worker.prompt), campaignDir),
        launcher,
        timeout: effectiveTimeout,
        complete_file: worker.complete_file
          ? resolvePath(String(worker.complete_file), campaignDir)
          : resolvePath(`workers/${workerId}-COMPLETE.yaml`, campaignDir),
        context_file: worker.context_file
          ? resolvePath(String(worker.context_file), campaignDir)
          : undefined,
      };
    });

    const rawGate =
      phase.gate as Array<Record<string, unknown>> | undefined;
    const gate: CampaignGateCheck[] | undefined = rawGate?.map(
      (check, gi) => {
        validateRequired(check, "name", `phases[${pi}].gate[${gi}]`);
        validateRequired(check, "command", `phases[${pi}].gate[${gi}]`);
        return {
          name: String(check.name),
          command: String(check.command),
          run_on: (check.run_on as "laptop" | "hearth") ?? "laptop",
        };
      },
    );

    return {
      name: phaseName,
      sequential: phase.sequential !== false, // Default: true (sequential)
      depends_on: phase.depends_on as string[] | undefined,
      workers,
      gate,
    };
  });

  // --- Success criteria ---
  const successCriteria = raw.success_criteria as string[] | undefined;

  return {
    campaign: {
      name: campaignName,
      version: campaignVersion,
    },
    preflight,
    phases,
    success_criteria: successCriteria,
  };
}
