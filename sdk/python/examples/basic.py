#!/usr/bin/env python3
"""A small tour of the RAVEN Python SDK against a live local stack
(docker compose up). Registers a throwaway user, submits a webhook job,
watches it to a terminal state and lists workers.

Run from the sdk/python directory:

    python examples/basic.py --email demo@example.com --password demo12345
"""

import argparse

from raven import Client, RavenError


def main() -> None:
    parser = argparse.ArgumentParser(description="RAVEN Python SDK tour")
    parser.add_argument("--base", default="http://localhost:8080", help="gateway base URL")
    parser.add_argument("--email", default="demo@example.com")
    parser.add_argument("--password", default="demo12345", help="8+ chars")
    args = parser.parse_args()

    client = Client(base_url=args.base)

    # Register is idempotent-friendly for demos: if the email is taken we
    # just log in with the same credentials.
    try:
        client.register(args.email, args.password, display_name="SDK Demo")
    except RavenError as err:
        print(f"register: {err} (continuing to login)")

    client.login(args.email, args.password)
    print("logged in")

    job = client.create_job(
        type="webhook",
        payload={"url": "https://example.com/hook", "event": "sdk.demo"},
    )
    print(f"job {job.id} created, status {job.status}")

    final = client.watch_job(job.id, interval=2.0, timeout=120)
    print(f"job {final.id} finished as {final.status}")

    workers = client.list_workers()
    print(f"{len(workers)} live worker(s)")

    report = client.health_services()
    for svc in report.services:
        print(f"  {svc.name:<12} {svc.status:<9} {svc.detail}")

    client.logout()
    print("logged out")


if __name__ == "__main__":
    main()
