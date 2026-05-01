import csv
import email
import imaplib
import os
import re
from datetime import datetime
from email.header import decode_header
from email.message import Message
from pathlib import Path
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

CSV_PATH = Path("data/judge_teams_links.csv")
PROCESSED_UIDS_PATH = Path("processed_uids.txt")

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
    "message_uid",
]


def decode_mime_header(value: str) -> str:
    if not value:
        return ""

    parts = decode_header(value)
    decoded = ""

    for part, encoding in parts:
        if isinstance(part, bytes):
            decoded += part.decode(encoding or "utf-8", errors="replace")
        else:
            decoded += part

    return decoded.strip()


def get_email_body(msg: Message) -> str:
    plain_text = ""
    html_text = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))

            if "attachment" in disposition.lower():
                continue

            payload = part.get_payload(decode=True)
            if not payload:
                continue

            charset = part.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")

            if content_type == "text/plain":
                plain_text += "\n" + text
            elif content_type == "text/html":
                html_text += "\n" + text
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            if msg.get_content_type() == "text/html":
                html_text = payload.decode(charset, errors="replace")
            else:
                plain_text = payload.decode(charset, errors="replace")

    if plain_text.strip():
        return plain_text

    if html_text.strip():
        soup = BeautifulSoup(html_text, "html.parser")
        return soup.get_text("\n")

    return ""


def extract_teams_link(text: str) -> str:
    patterns = [
        r"https://teams\.microsoft\.com/[^\s<>\"]+",
        r"https://.*?\.teams\.microsoft\.com/[^\s<>\"]+",
        r"https://www\.microsoft\.com/.+?teams[^\s<>\"]+",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return clean_url(match.group(0))

    return ""


def clean_url(url: str) -> str:
    url = url.strip()
    url = url.rstrip(").,;]")
    url = url.replace("&amp;", "&")
    return url


def extract_judge(text: str) -> str:
    patterns = [
        r"(?:Judge|Hon\.?|Honorable)\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){0,4})",
        r"(?:before|with)\s+(?:Judge|Hon\.?|Honorable)\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){0,4})",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            judge = match.group(1).strip()
            judge = re.sub(r"\s+", " ", judge)
            return judge

    return ""


def extract_date_time(text: str):
    candidates = []

    patterns = [
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM|A\.M\.|P\.M\.)?\b",
        r"\b\d{1,2}-\d{1,2}-\d{2,4}\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM|A\.M\.|P\.M\.)?\b",
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM|A\.M\.|P\.M\.)?\b",
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            candidates.append(match.group(0))

    for candidate in candidates:
        try:
            parsed = date_parser.parse(candidate, fuzzy=True)
            court_date = parsed.strftime("%Y-%m-%d")
            court_time = parsed.strftime("%I:%M %p")
            return court_date, court_time
        except Exception:
            continue

    return "", ""


def load_processed_uids() -> set:
    if not PROCESSED_UIDS_PATH.exists():
        return set()

    return {
        line.strip()
        for line in PROCESSED_UIDS_PATH.read_text().splitlines()
        if line.strip()
    }


def save_processed_uid(uid: str):
    with PROCESSED_UIDS_PATH.open("a", encoding="utf-8") as file:
        file.write(uid + "\n")


def ensure_csv_exists():
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
            writer.writeheader()


def append_row(row: dict):
    ensure_csv_exists()

    with CSV_PATH.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
        writer.writerow(row)


def parse_message(uid: str, raw_email: bytes) -> dict:
    msg = email.message_from_bytes(raw_email)

    subject = decode_mime_header(msg.get("Subject", ""))
    sender = decode_mime_header(msg.get("From", ""))
    received_at = msg.get("Date", "")

    body = get_email_body(msg)
    full_text = f"{subject}\n{body}"

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

    parsed_status = "Parsed" if not missing else "Needs Review"

    return {
        "received_at": received_at,
        "email_from": sender,
        "email_subject": subject,
        "judge": judge,
        "court_date": court_date,
        "court_time": court_time,
        "teams_link": teams_link,
        "parsed_status": parsed_status,
        "notes": "; ".join(missing),
        "message_uid": uid,
    }


def main():
    imap_host = os.environ["IMAP_HOST"]
    imap_user = os.environ["IMAP_USER"]
    imap_password = os.environ["IMAP_PASSWORD"]
    imap_folder = os.environ.get("IMAP_FOLDER", "INBOX")

    processed_uids = load_processed_uids()
    ensure_csv_exists()

    mail = imaplib.IMAP4_SSL(imap_host)
    mail.login(imap_user, imap_password)
    mail.select(imap_folder)

    status, data = mail.uid("search", None, "ALL")
    if status != "OK":
        raise RuntimeError("Unable to search mailbox.")

    uids = data[0].decode().split()

    for uid in uids:
        if uid in processed_uids:
            continue

        status, msg_data = mail.uid("fetch", uid, "(BODY.PEEK[])")
        if status != "OK":
            continue

        raw_email = msg_data[0][1]
        row = parse_message(uid, raw_email)

        if row["teams_link"]:
            append_row(row)
            save_processed_uid(uid)
        else:
            append_row(row)
            save_processed_uid(uid)

    mail.logout()


if __name__ == "__main__":
    main()
