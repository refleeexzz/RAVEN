"""check uncached go test -json results for the required integration suites."""

import json
import sys


INTEGRATION = "github.com/refleeexzz/RAVEN/tests/integration"
SECURITY = "github.com/refleeexzz/RAVEN/tests/security"
REQUIRED_REGRESSIONS = (
    "TestWorkerCrashesExhaustAttemptBudget",
    "TestRetryLeaseExpiryPreservesAttemptBudget",
    "TestImmediateDispatchSurvivesJobsProcessCrash",
    "TestImmediateDispatchAcceptanceAndRollback",
    "TestImmediateDispatchMigration",
    "TestScheduledDispatchRecoversAfterBrokerFailure",
    "TestScheduledDispatchDoesNotPublishBeforeCommit",
    "TestScheduledDispatchRetriesAfterAck",
)


def main():
    passed = {INTEGRATION: set(), SECURITY: set()}
    completed = set()
    problems = []
    for line in sys.stdin:
        event = json.loads(line)
        sys.stdout.write(event.get("Output", ""))
        package = event.get("Package")
        if package not in passed:
            continue
        test = event.get("Test", "")
        action = event["Action"]
        if action in ("fail", "skip"):
            problems.append(f"{action}: {package}/{test}" if test else f"{action}: {package}")
        elif action == "pass":
            if not test:
                completed.add(package)
            elif "/" not in test:
                passed[package].add(test)

    for package, tests in passed.items():
        print(f"{package}: {len(tests)} top-level tests passed")
        if package not in completed or not tests:
            problems.append(f"Missing successful test suite: {package}")
    for name in REQUIRED_REGRESSIONS:
        if name not in passed[INTEGRATION]:
            problems.append(f"Missing successful regression: {INTEGRATION}/{name}")
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
