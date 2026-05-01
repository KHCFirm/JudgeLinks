import csv
import os
import re
from pathlib import Path

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
    return {x.strip() for x in PROCESSED_PATH.read_text().splitlines() if x.strip()}


def save_processed_id(message_id):
    with PROCESSED_PATH.open("a", encoding="utf-8") as f:
        f.write(message_id + "\n")


def append_row(row):
    ensure_csv_exists()
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)


def get_graph_token():
    tenant_id = os.environ["TENANT_ID"]
    client_id = os.environ["CLIENT_ID"]
    client_secret = os.environ["CLIENT_SECRET"]

    response = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        },
        timeout=30,
    )

    if not response.ok:
        print(response.text)
        response.raise_for_status()

    return response.json()["access_token"]


def html_to_text(html):
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text("\n")


def normalize_text(text):
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


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


def subject_without_forward_prefix(subject):
    subject = subject or ""
    subject = re.sub(r"^(fw|fwd|re):\s*", "", subject.strip(), flags=re.I)
    return subject.strip()


def extract_subject_date(subject):
    clean_subject = subject_without_forward_prefix(subject)

    match = re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", clean_subject)
    if not match:
        return ""

    try:
        parsed = date_parser.parse(match.group(0), fuzzy=True)
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        return ""


def get_context_around_teams_link(text):
    link = extract_teams_link(text)
    if not link:
        return text

    index = text.find(link)
    if index == -1:
        return text

    start = max(0, index - 800)
    end = min(len(text), index + len(link) + 1200)
    return text[start:end]


def extract_judge_from_signature(text):
    patterns = [
        r"\bHon\.?\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][A-Za-z'\-]+){1,3})\b",
        r"\bHonorable\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][A-Za-z'\-]+){1,3})\b",
    ]

    for pattern in patterns:
        matches = list(re.finditer(pattern, text, re.I))
        for match in matches:
            name = clean_judge_name(match.group(1))
            if is_reasonable_judge_name(name):
                return name

    return ""


def extract_judge_from_from_header(text):
    # Handles: From: Prisco, Robert [DOL] <Robert.Prisco@dol.nj.gov>
    pattern = r"From:\s*([A-Z][A-Za-z'\-]+),\s*([A-Z][A-Za-z'\-]+)(?:\s+[A-Z]\.)?"
    matches = list(re.finditer(pattern, text))

    for match in matches:
        last = match.group(1).strip()
        first = match.group(2).strip()
        name = f"{first} {last}"
        if is_reasonable_judge_name(name):
            return name

    return ""


def extract_judge_from_subject(subject):
    clean_subject = subject_without_forward_prefix(subject)

    # Stops at TEAMS/link/date instead of swallowing the rest of the subject.
    match = re.search(
        r"\bJudge\s+(.+?)(?=\s+(?:TEAMS?|Zoom|link|meeting|\d{1,2}/\d{1,2}/\d{2,4})\b|$)",
        clean_subject,
        re.I,
    )

    if match:
        name = clean_judge_name(match.group(1))
        if is_reasonable_judge_name(name):
            return name

    return ""


def clean_judge_name(name):
    name = re.sub(r"\[[^\]]+\]", "", name)
    name = re.sub(r"\b(TEAMS?|Zoom|link|meeting|court|list)\b.*$", "", name, flags=re.I)
    name = re.sub(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", "", name)
    name = re.sub(r"\s+", " ", name)
    name = name.strip(" ,.-")

    # Keep first, middle initial, last. Drop titles/noise.
    parts = name.split()
    filtered = []
    for part in parts:
        if part.lower() in {"judge", "hon", "honorable", "teams", "link"}:
            continue
        filtered.append(part)

    return " ".join(filtered).strip()


def is_reasonable_judge_name(name):
    if not name:
        return False

    bad_words = {
        "teams",
        "link",
        "court",
        "marking",
        "please",
        "accept",
        "following",
        "sent",
        "from",
        "subject",
        "department",
    }

    parts = name.split()

    if len(parts) < 2 or len(parts) > 4:
        return False

    if any(part.lower().strip(".,") in bad_words for part in parts):
        return False

    if any(re.search(r"\d", part) for part in parts):
        return False

    return True


def extract_judge(subject, text):
    context = get_context_around_teams_link(text)

    for extractor in [
        lambda: extract_judge_from_signature(context),
        lambda: extract_judge_from_from_header(text),
        lambda: extract_judge_from_subject(subject),
        lambda: extract_judge_from_signature(text),
    ]:
        value = extractor()
        if value:
            return value

    return ""


def extract_time_near_teams_link(text):
    context = get_context_around_teams_link(text)

    patterns = [
        r"\bI\s+start\s+at\s+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b",
        r"\bstart\s+at\s+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b",
        r"\bbegin\s+at\s+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b",
        r"\b(\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?))\b",
        r"\b(\d{1,2}\s*(?:a\.?m\.?|p\.?m\.?))\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, context, re.I)
        if match:
            raw_time = match.group(1)
            try:
                parsed = date_parser.parse(raw_time, fuzzy=True)
                return parsed.strftime("%I:%M %p")
            except Exception:
                pass

    return ""


def extract_date_time(subject, text):
    court_date = extract_subject_date(subject)
    court_time = extract_time_near_teams_link(text)

    if court_date or court_time:
        return court_date, court_time

    # Fallback only. Avoid Sent: dates when possible.
    cleaned = re.sub(
        r"Sent:\s*(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?.*?\d{1,2}:\d{2}\s*(AM|PM)",
        "",
        text,
        flags=re.I,
    )

    patterns = [
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM|A\.M\.|P\.M\.)?\b",
        r"\b\d{1,2}-\d{1,2}-\d{2,4}\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM|A\.M\.|P\.M\.)?\b",
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM|A\.M\.|P\.M\.)?\b",
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, cleaned, re.I):
            try:
                parsed = date_parser.parse(match.group(0), fuzzy=True)
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

    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )

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

    body_text = normalize_text(body_text)
    full_text = normalize_text(f"{subject}\n{body_text}")

    teams_link = extract_teams_link(full_text)
    judge = extract_judge(subject, full_text)
    court_date, court_time = extract_date_time(subject, full_text)

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
            f"time={row['court_time']!r}, "
            f"teams_found={bool(row['teams_link'])}"
        )

        new_count += 1

    print(f"New messages processed: {new_count}")


if __name__ == "__main__":
    main()
