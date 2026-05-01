import csv
import os
import re
from pathlib import Path
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

CSV_PATH = Path("data/judge_teams_links.csv")
PROCESSED_PATH = Path("processed_message_ids.txt")

CSV_HEADERS = [
    "received_at",
    "email_from",
    "email_subject",
    "judge",
    "court_date",
    "court_time",
    "teams_link",
    "parsed_status",
    "notes",
    "message_id",
]


def ensure_csv_exists():
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()


def load_processed_ids():
    if not PROCESSED_PATH.exists():
        return set()
    return set(x.strip() for x in PROCESSED_PATH.read_text().splitlines() if x.strip())


def save_processed_id(message_id):
    with PROCESSED_PATH.open("a", encoding="utf-8") as f:
        f.write(message_id + "\n")


def append_row(row):
    ensure_csv_exists()
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writerow(row)


def get_graph_token():
    tenant_id = os.environ["TENANT_ID"]
    client_id = os.environ["CLIENT_ID"]
    client_secret = os.environ["CLIENT_SECRET"]

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    response = requests.post(url, data=data, timeout=30)
    if not response.ok:
        print(response.text)
        response.raise_for_status()

    return response.json()["access_token"]


def html_to_text(html):
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text("\n")


def clean_url(url):
    return url.strip().rstrip(").,;]").replace("&amp;", "&")


def extract_teams_link(text):
    patterns = [
        r"https://teams\.microsoft\.com/[^\s<>\"]+",
        r"https://.*?\.teams\.microsoft\.com/[^\s<>\"]+",
        r"https://www\.microsoft\.com/.+?teams[^\s<>\"]+",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return clean_url(match.group(0))

    return ""


def extract_judge(text):
    patterns = [
        r"(?:Judge|Hon\.?|Honorable)\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){0,4})",
        r"(?:before|with)\s+(?:Judge|Hon\.?|Honorable)\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){0,4})",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return re.sub(r"\s+", " ", match.group(1)).strip()

    return ""


def extract_date_time(text):
    patterns = [
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM|A\.M\.|P\.M\.)?\b",
        r"\b\d{1,2}-\d{1,2}-\d{2,4}\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM|A\.M\.|P\.M\.)?\b",
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM|A\.M\.|P\.M\.)?\b",
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    ]

    for pattern in patterns:
        matches = re.findall(pattern, text, re.I)
        for value in matches:
            if isinstance(value, tuple):
                value = " ".join(value)
            try:
                parsed = date_parser.parse(value, fuzzy=True)
                return parsed.strftime("%Y-%m-%d"), parsed.strftime("%I:%M %p")
            except Exception:
                pass

    return "", ""


def get_recent_messages(token):
    mailbox = os.environ["MAILBOX_USER"]

    url = (
        f"https://graph.microsoft.com/v1.0/users/{mailbox}"
        "/mailFolders/Inbox/messages"
        "?$top=25"
        "&$orderby=receivedDateTime desc"
        "&$select=id,receivedDateTime,from,subject,body"
    )

    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers, timeout=30)
    if not response.ok:
        print(response.text)
        response.raise_for_status()

    return response.json().get("value", [])


def parse_message(message):
    subject = message.get("subject", "") or ""
    received_at = message.get("receivedDateTime", "") or ""
    sender = (
        message.get("from", {})
        .get("emailAddress", {})
        .get("address", "")
    )

    body_obj = message.get("body", {}) or {}
    body_content = body_obj.get("content", "") or ""
    body_type = body_obj.get("contentType", "")

    if body_type.lower() == "html":
        body_text = html_to_text(body_content)
    else:
        body_text = body_content

    full_text = f"{subject}\n{body_text}"

    teams_link = extract_teams_link(full_text)
    judge = extract_judge(full_text)
    court_date, court_time = extract_date_time(full_text)

    missing = []
    if not judge:
        missing.append("Missing Judge")
    if not court_date:
        missing.append("Missing Date")
    if not court_time:
        missing.append("Missing Time")
    if not teams_link:
        missing.append("Missing Teams Link")

    return {
        "received_at": received_at,
        "email_from": sender,
        "email_subject": subject,
        "judge": judge,
        "court_date": court_date,
        "court_time": court_time,
        "teams_link": teams_link,
        "parsed_status": "Parsed" if not missing else "Needs Review",
        "notes": "; ".join(missing),
        "message_id": message["id"],
    }


def main():
    ensure_csv_exists()

    print("Getting Microsoft Graph token...")
    token = get_graph_token()
    print("Token acquired.")

    print("Loading processed message IDs...")
    processed_ids = load_processed_ids()

    print("Getting recent emails from mailbox...")
    messages = get_recent_messages(token)
    print(f"Messages found: {len(messages)}")

    new_count = 0

    for message in reversed(messages):
        message_id = message["id"]

        if message_id in processed_ids:
            continue

        row = parse_message(message)
        append_row(row)
        save_processed_id(message_id)

        print(
            f"Processed: subject={row['email_subject']!r}, "
            f"judge={row['judge']!r}, "
            f"date={row['court_date']!r}, "
            f"teams_found={bool(row['teams_link'])}"
        )

        new_count += 1

    print(f"New messages processed: {new_count}")


if __name__ == "__main__":
    main()
