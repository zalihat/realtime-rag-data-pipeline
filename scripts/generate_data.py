#!/usr/bin/env python3
"""
generate_data.py

Generates realistic-ish customers, support tickets, and ticket resolution
notes, and inserts them directly into Postgres (matching the schema in
infra/postgres/init.sql).

Usage:
    python generate_data.py --customers 50 --tickets 200 --resolve-pct 0.7

Env vars (override with flags if you prefer):
    PGHOST=localhost
    PGPORT=5432
    PGDATABASE=supportdb
    PGUSER=appuser
    PGPASSWORD=apppass
"""

import argparse
import os
import random
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

fake = Faker()

# ---------------------------------------------------------------------------
# Ticket "categories" -- each has a pool of subjects, body templates, and
# matching resolution note templates so the ticket <-> resolution pairing
# actually makes topical sense (useful for RAG retrieval testing later).
# ---------------------------------------------------------------------------

CATEGORIES = {
    "billing": {
        "subjects": [
            "Charged twice for my subscription",
            "Invoice doesn't match my plan",
            "Refund request for last billing cycle",
            "Can't update my payment method",
            "Unexpected charge on my card",
        ],
        "body_templates": [
            "Hi, I noticed I was charged {amount} on {date} but I only expected to pay {expected}. Can you look into this?",
            "My last invoice shows a charge of {amount} for a plan I downgraded from weeks ago. Please correct this.",
            "I tried updating my card on file but keep getting an error at checkout. My account email is {email}.",
            "I was billed {amount} twice this month for the same subscription period. Requesting a refund for the duplicate.",
        ],
        "resolutions": [
            "Confirmed duplicate charge of {amount} in billing system; issued refund to original payment method, should post within 5-7 business days.",
            "Reviewed account and found plan downgrade wasn't applied to billing cycle due to a sync delay. Manually corrected invoice and applied {amount} credit.",
            "Updated payment method on file for the customer after confirming identity. Card ending correctly reflects new details now.",
            "Root cause was a proration bug on plan change. Applied one-time credit of {amount} and confirmed with customer via email.",
        ],
    },
    "login": {
        "subjects": [
            "Can't log into my account",
            "Password reset email never arrived",
            "Account locked after failed attempts",
            "2FA code not being accepted",
        ],
        "body_templates": [
            "I've tried logging in several times and keep getting 'invalid credentials' even after resetting my password.",
            "I requested a password reset link over an hour ago and still haven't received the email. Checked spam too.",
            "My account got locked after a few failed login attempts. Can someone unlock it? Email is {email}.",
            "The 2FA code from my authenticator app keeps getting rejected as invalid.",
        ],
        "resolutions": [
            "Reset password reset token manually and re-sent the email; confirmed customer received it and regained access.",
            "Found account lockout was triggered by an old saved password on customer's browser. Unlocked account and walked them through clearing cache.",
            "Identified clock drift on customer's authenticator app causing 2FA mismatch. Had them resync the app time and issue resolved.",
            "Escalated to auth team, found email deliverability issue with customer's provider. Whitelisted our sending domain and resent link successfully.",
        ],
    },
    "bug": {
        "subjects": [
            "Dashboard not loading",
            "Export to CSV is broken",
            "Data missing after sync",
            "App crashes on mobile",
        ],
        "body_templates": [
            "The dashboard just shows a blank white screen since this morning. Tried refreshing and clearing cache, no luck.",
            "When I try to export my report to CSV, the download starts but the file is empty.",
            "Some of my records from last week are missing after the latest sync ran.",
            "The mobile app crashes immediately every time I open the 'Reports' tab.",
        ],
        "resolutions": [
            "Identified a null pointer exception in the dashboard rendering service triggered by an empty widget config; deployed hotfix.",
            "Found the CSV export worker was timing out on larger datasets and silently returning an empty file. Increased timeout and added error handling.",
            "Traced missing records to a failed sync job that errored out midway without alerting. Reran the sync manually and verified data is now complete.",
            "Reproduced the crash, found it was caused by a null date field in report data. Shipped a patch in the next app release.",
        ],
    },
    "feature_request": {
        "subjects": [
            "Would love a dark mode option",
            "Request: bulk export for tickets",
            "Can we get Slack notifications?",
            "Ability to filter reports by custom date range",
        ],
        "body_templates": [
            "Just a suggestion, but a dark mode toggle would be great for our team since we use the app late at night.",
            "It'd be really helpful to bulk export all resolved tickets instead of one at a time.",
            "Any chance you could add Slack notifications when a ticket status changes?",
            "Right now I can only filter by preset date ranges. Custom range would help a lot for our reporting.",
        ],
        "resolutions": [
            "Logged this as a feature request with the product team and added it to the backlog for review.",
            "Passed this along to engineering; noted as a common ask from multiple customers this quarter.",
            "Added to our public roadmap under 'notifications' -- thanked the customer for the suggestion.",
            "Confirmed this is already planned for an upcoming release and let the customer know the rough timeline.",
        ],
    },
    "shipping": {
        "subjects": [
            "Order hasn't arrived yet",
            "Wrong item received",
            "Package marked delivered but never arrived",
            "Need to change shipping address",
        ],
        "body_templates": [
            "My order was supposed to arrive 3 days ago and tracking hasn't updated since. Can you check on this?",
            "I received the wrong item in my package -- ordered {expected} but got something else entirely.",
            "Tracking says my package was delivered yesterday but I never received it. Checked with neighbors, nothing.",
            "I need to change my shipping address before the order ships out, it's still showing as processing.",
        ],
        "resolutions": [
            "Contacted the carrier and confirmed the package was delayed at a regional facility; provided updated tracking and ETA to customer.",
            "Confirmed wrong item was shipped due to a warehouse picking error. Shipped correct item overnight at no cost and provided return label.",
            "Filed a claim with the carrier for the lost package and shipped a free replacement to the customer.",
            "Updated the shipping address before the order left the warehouse and confirmed the change with the customer.",
        ],
    },
}

AGENTS = [
    "Priya Nair", "James Okafor", "Maria Gonzalez", "Tom Chen",
    "Aisha Bello", "Daniel Kim", "Fatima Yusuf", "Liam O'Connor",
]


def gen_customers(n):
    rows = []
    for _ in range(n):
        name = fake.name()
        email = fake.unique.email()
        created_at = fake.date_time_between(start_date="-1y", end_date="-1d")
        rows.append((name, email, created_at))
    return rows


def gen_ticket_body(category):
    template = random.choice(CATEGORIES[category]["body_templates"])
    return template.format(
        amount=f"${random.randint(9, 299)}.{random.randint(0, 99):02d}",
        expected=f"${random.randint(9, 299)}.{random.randint(0, 99):02d}",
        date=fake.date_this_year().isoformat(),
        email=fake.email(),
    )


def gen_resolution_note(category):
    template = random.choice(CATEGORIES[category]["resolutions"])
    return template.format(amount=f"${random.randint(9, 299)}.{random.randint(0, 99):02d}")


def gen_tickets(n, customer_ids, resolve_pct):
    tickets = []       # (customer_id, subject, body, status, created_at, updated_at, category)
    for _ in range(n):
        category = random.choice(list(CATEGORIES.keys()))
        subject = random.choice(CATEGORIES[category]["subjects"])
        body = gen_ticket_body(category)
        created_at = fake.date_time_between(start_date="-6M", end_date="-1d")
        will_resolve = random.random() < resolve_pct
        if will_resolve:
            updated_at = created_at + timedelta(hours=random.randint(1, 96))
            status = "resolved"
        else:
            updated_at = created_at
            status = "open"
        tickets.append((
            random.choice(customer_ids), subject, body, status,
            created_at, updated_at, category, will_resolve,
        ))
    return tickets


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic support ticket data")
    parser.add_argument("--customers", type=int, default=50)
    parser.add_argument("--tickets", type=int, default=200)
    parser.add_argument("--resolve-pct", type=float, default=0.7,
                         help="Fraction of tickets that get a resolution note (0-1)")
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE", "supportdb"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "appuser"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", "apppass"))
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.dbname,
        user=args.user, password=args.password,
    )
    conn.autocommit = False
    cur = conn.cursor()
    print("found connection")
    try:
        # --- customers ---
        customers = gen_customers(args.customers)
        customer_ids = execute_values(
            cur,
            "INSERT INTO customers (name, email, created_at) VALUES %s RETURNING customer_id",
            customers,
            fetch=True,
        )
        customer_ids = [row[0] for row in customer_ids]
        print(f"Inserted {len(customer_ids)} customers")

        # --- tickets ---
        ticket_rows = gen_tickets(args.tickets, customer_ids, args.resolve_pct)
        ticket_insert_values = [
            (cid, subject, body, status, created_at, updated_at)
            for cid, subject, body, status, created_at, updated_at, _cat, _resolve in ticket_rows
        ]
        ticket_ids = execute_values(
            cur,
            """INSERT INTO tickets (customer_id, subject, body, status, created_at, updated_at)
               VALUES %s RETURNING ticket_id""",
            ticket_insert_values,
            fetch=True,
        )
        ticket_ids = [row[0] for row in ticket_ids]
        print(f"Inserted {len(ticket_ids)} tickets")

        # --- resolutions (only for tickets marked resolved) ---
        resolution_rows = []
        for ticket_id, (cid, subject, body, status, created_at, updated_at, category, will_resolve) in zip(ticket_ids, ticket_rows):
            if will_resolve:
                note = gen_resolution_note(category)
                agent = random.choice(AGENTS)
                resolution_rows.append((ticket_id, agent, note, updated_at))

        if resolution_rows:
            execute_values(
                cur,
                """INSERT INTO ticket_resolutions (ticket_id, agent_name, note, resolved_at)
                   VALUES %s""",
                resolution_rows,
            )
        print(f"Inserted {len(resolution_rows)} ticket resolutions")

        conn.commit()
        print("Done.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()