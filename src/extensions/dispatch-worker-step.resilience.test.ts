/**
 * Resilience Tests for dispatch-worker-step and campaign-spec-parser
 *
 * Tests the pure logic of rate-limit detection, timeout floors, and
 * status semantics without requiring SSH, tmux, or postgres.
 *
 * Run: npx tsx src/extensions/dispatch-worker-step.resilience.test.ts
 */

// --- Rate-limit pattern matching ---

const RATE_LIMIT_RE =
  /rate.?limit|capacity|exceeded|too many requests|429|overloaded|throttl/i;

const RATE_LIMIT_POSITIVE_CASES = [
  "Error: rate limit exceeded for this account",
  "You have exceeded your rate limit",
  "HTTP 429 Too Many Requests",
  "too many requests, please try again later",
  "The API is currently overloaded",
  "Request throttled by API gateway",
  "Account capacity has been reached",
  "rate-limit applied to your organization",
  "RateLimit: exceeded 100 requests per minute",
];

const RATE_LIMIT_NEGATIVE_CASES = [
  "Claude Code is ready to help",
  "Reading file /Users/matthewchapin/project/main.ts",
  "Welcome to the team, brother",
  "Running tests... 42 passed",
  "Error: file not found",
  "Permission denied: /etc/shadow",
  "Compilation successful with 0 errors",
];

// --- Timeout floor ---

const MIN_TIMEOUT_SECONDS = 1800;
const DEFAULT_TIMEOUT_SECONDS = 3600;

function applyTimeoutFloor(rawTimeout: number | undefined): number {
  const timeout = typeof rawTimeout === "number" ? rawTimeout : DEFAULT_TIMEOUT_SECONDS;
  return Math.max(timeout, MIN_TIMEOUT_SECONDS);
}

// --- Test runner ---

let passed = 0;
let failed = 0;

function assert(condition: boolean, message: string): void {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${message}`);
  }
}

// Test 1: Rate-limit pattern detection — positive cases
console.log("Test 1: Rate-limit detection — positive cases");
for (const text of RATE_LIMIT_POSITIVE_CASES) {
  assert(
    RATE_LIMIT_RE.test(text),
    `Should detect rate-limit in: "${text.slice(0, 60)}"`,
  );
}

// Test 2: Rate-limit pattern detection — negative cases
console.log("Test 2: Rate-limit detection — negative cases");
for (const text of RATE_LIMIT_NEGATIVE_CASES) {
  assert(
    !RATE_LIMIT_RE.test(text),
    `Should NOT flag as rate-limit: "${text.slice(0, 60)}"`,
  );
}

// Test 3: Timeout floor enforcement
console.log("Test 3: Timeout floor enforcement");
assert(applyTimeoutFloor(749) === 1800, "749s should be raised to 1800s");
assert(applyTimeoutFloor(600) === 1800, "600s should be raised to 1800s");
assert(applyTimeoutFloor(1800) === 1800, "1800s should stay at 1800s");
assert(applyTimeoutFloor(3600) === 3600, "3600s should stay at 3600s");
assert(applyTimeoutFloor(7200) === 7200, "7200s should stay at 7200s");
assert(applyTimeoutFloor(undefined) === 3600, "undefined should default to 3600s");
assert(applyTimeoutFloor(0) === 1800, "0 should be raised to 1800s");
assert(applyTimeoutFloor(-1) === 1800, "negative should be raised to 1800s");

// Test 4: Campaign status semantics
console.log("Test 4: Campaign status semantics");
function computeCampaignStatus(
  phasesCompleted: number,
  phasesTotal: number,
): "completed" | "partial" | "failed" {
  return phasesCompleted === phasesTotal
    ? "completed"
    : phasesCompleted > 0
      ? "partial"
      : "failed";
}
assert(computeCampaignStatus(6, 6) === "completed", "6/6 phases = completed");
assert(computeCampaignStatus(2, 6) === "partial", "2/6 phases = partial (not completed!)");
assert(computeCampaignStatus(0, 6) === "failed", "0/6 phases = failed");
assert(computeCampaignStatus(1, 1) === "completed", "1/1 phases = completed");
assert(computeCampaignStatus(0, 1) === "failed", "0/1 phases = failed");

// Test 5: Cloud launcher rotation
console.log("Test 5: Cloud launcher rotation");
const CLOUD_LAUNCHERS = ["cpb", "cbb", "cmb", "clb", "c5b"];
const triedLaunchers = new Set(["cpb"]);
const nextLauncher = CLOUD_LAUNCHERS.find((l) => !triedLaunchers.has(l));
assert(nextLauncher === "cbb", "After cpb, next should be cbb");

triedLaunchers.add("cbb");
const nextLauncher2 = CLOUD_LAUNCHERS.find((l) => !triedLaunchers.has(l));
assert(nextLauncher2 === "cmb", "After cpb+cbb, next should be cmb");

triedLaunchers.add("cmb");
triedLaunchers.add("clb");
triedLaunchers.add("c5b");
const nextLauncher3 = CLOUD_LAUNCHERS.find((l) => !triedLaunchers.has(l));
assert(nextLauncher3 === undefined, "All launchers tried = undefined");

// --- Summary ---
console.log(`\nResults: ${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
