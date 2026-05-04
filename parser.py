import csv
import html
import os
import re
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

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

COMMON_JUDGES = [
    "Joseph W. Borucki",
    "R. Louis Gallagher",
    "Theresa Yang",
    "David H. Puma",
    "Carmine Taglialatella",
    "Robert Sebera",
    "Ingrid French",
    "Elizabeth White",
    "James Robertson",
    "David Laporta",
    "Gerald H Massell",
    "William Roca",
    "James Arsenault",
    "Robert Prisco",
    "Diana Montes",
    "Peter J Koulikourdis",
    "Francis G. Reuss",
    "Glenn Kaplan",
    "David R. Puma",
    "Thomas Capotorto",
    "Robert Thuring",
    "Dawn Shanahan",
    "Neme Akunne",
    "Tanya Phillips",
    "Willam Feingold",
    "Michael Dillon",
    "Ashley Hutchinson",
    "Mary H. Casey",
    "John Rodriguez",
    "David Lande",
    "Fred Hopke",
    "Dana Mayo",
    "Phillip Laporta",
    "April Gilmore",
    "Thomas Smith",
    "Salvatore Martino",
    "Walter Schneider",
    "Bonnie Kass-Viola",
    "Christopher Leitner",
    "Brian Eyerman",
    "Maria Del Valle-Koch",
    "Diana Ferriero",
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
    response = requests.post(
        f"https://login.microsoftonline.com/{os.environ['TENANT_ID']}/oauth2/v2.0/token",
        data={
            "client_id": os.environ["CLIENT_ID"],
            "client_secret": os.environ["CLIENT_SECRET"],
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    if not response.ok:
        print(response.text)
        response.raise_for_status()
    return response.json()["access_token"]


def normalize_text(text):
    text = text or ""
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = text.replace("=92", "'")
    text = text.replace("’", "'")
    text = text.replace("–", "-")
    text = text.replace("—", "-")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def html_to_text_with_links(html_content):
    if not html_content:
        return "", []

    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text("\n")

    links = []
    for tag in soup.find_all("a", href=True):
        href = html.unescape(tag.get("href", "")).strip()
        if href:
            links.append(href)

    return normalize_text(text), links


def subject_without_forward_prefix(subject):
    subject = subject or ""
    subject = re.sub(r"^(\s*(fw|fwd|re):\s*)+", "", subject.strip(), flags=re.I)
    subject = subject.replace("[EXTERNAL]", "").strip()
    return subject


def clean_url(url):
    url = html.unescape(url or "")
    url = unquote(url)
    url = url.strip()
    url = url.rstrip(").,;]'\">")
    url = url.replace("&amp;", "&")
    return url


def unwrap_protected_link(url):
    url = clean_url(url)

    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    if "url" in query and query["url"]:
        possible = clean_url(query["url"][0])
        if "teams.microsoft.com" in possible:
            return possible

    if "teams.microsoft.com" in url:
        return url

    return url


def extract_teams_link_from_values(values):
    direct_patterns = [
        r"https://teams\.microsoft\.com/[^\s<>\"]+",
        r"https://teams\.live\.com/[^\s<>\"]+",
        r"https://.*?\.teams\.microsoft\.com/[^\s<>\"]+",
        r"https://aka\.ms/[^\s<>\"]+",
    ]

    for value in values:
        if not value:
            continue

        value = html.unescape(str(value))
        value = unquote(value)

        for pattern in direct_patterns:
            match = re.search(pattern, value, re.I)
            if match:
                found = unwrap_protected_link(match.group(0))
                if "aka.ms" not in found.lower():
                    return clean_url(found)

        # Microsoft Safe Links often wrap the real Teams URL in a url= parameter.
        if "safelinks.protection.outlook.com" in value.lower() or "url=" in value.lower():
            found = unwrap_protected_link(value)
            if "teams.microsoft.com" in found.lower():
                return clean_url(found)

        # Proofpoint / URLDefense format may include the real URL inside the text.
        if "urldefense" in value.lower() and "teams.microsoft.com" in value.lower():
            match = re.search(r"https:.*?teams\.microsoft\.com.*", value, re.I)
            if match:
                return clean_url(match.group(0))

    return ""


def extract_teams_link(full_text, html_links=None, raw_html=""):
    html_links = html_links or []

    candidates = []
    candidates.extend(html_links)
    candidates.append(raw_html)
    candidates.append(full_text)

    return extract_teams_link_from_values(candidates)


def get_context_around_teams_link(text, teams_link=""):
    link = teams_link or extract_teams_link(text)
    if not link:
        return text

    idx = text.find(link)
    if idx == -1:
        return text

    return text[max(0, idx - 2500): min(len(text), idx + len(link) + 2500)]


def normalize_name_for_match(value):
    value = value.lower()
    value = re.sub(r"\bjudge\b", "", value)
    value = re.sub(r"\bhonorable\b", "", value)
    value = re.sub(r"\bhon\b", "", value)
    value = re.sub(r"[^a-z\s]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def judge_last_name(judge_name):
    clean = normalize_name_for_match(judge_name)
    parts = clean.split()
    return parts[-1] if parts else ""


def clean_judge_name(name):
    name = name or ""
    name = re.sub(r"\[[^\]]+\]", "", name)
    name = re.sub(
        r"\b(TEAMS?|Zoom|link|meeting|court|list|markings?|listed|settlement|paperwork)\b.*$",
        "",
        name,
        flags=re.I,
    )
    name = re.sub(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", "", name)
    name = re.sub(r"\s+", " ", name).strip(" ,.-:")

    parts = []
    for part in name.split():
        if part.lower().strip(".") in {"judge", "hon", "honorable", "your", "honor"}:
            continue
        parts.append(part)

    return " ".join(parts).strip()


def is_reasonable_judge_name(name):
    if not name:
        return False

    bad_words = {
        "teams", "link", "court", "marking", "markings", "please", "accept",
        "following", "sent", "from", "subject", "department", "microsoft",
        "meeting", "listed", "settlement", "paperwork", "conference", "today",
        "tomorrow", "dear", "your", "honor"
    }

    parts = name.split()

    if len(parts) < 1 or len(parts) > 4:
        return False

    if any(part.lower().strip(".,") in bad_words for part in parts):
        return False

    if any(re.search(r"\d", part) for part in parts):
        return False

    return True


def normalize_judge_name(candidate):
    candidate = clean_judge_name(candidate)
    if not candidate:
        return ""

    candidate_norm = normalize_name_for_match(candidate)
    candidate_parts = candidate_norm.split()

    if not candidate_parts:
        return ""

    candidate_last = candidate_parts[-1]

    best_name = ""
    best_score = 0

    for judge in COMMON_JUDGES:
        judge_norm = normalize_name_for_match(judge)
        score = SequenceMatcher(None, candidate_norm, judge_norm).ratio()

        if score > best_score:
            best_score = score
            best_name = judge

    if best_score >= 0.82:
        return best_name

    matches = [
        judge for judge in COMMON_JUDGES
        if judge_last_name(judge) == candidate_last
    ]

    if len(matches) == 1:
        return matches[0]

    if len(candidate_parts) >= 2:
        first = candidate_parts[0]
        last = candidate_parts[-1]

        matches = [
            judge for judge in COMMON_JUDGES
            if normalize_name_for_match(judge).split()[0] == first
            and judge_last_name(judge) == last
        ]

        if len(matches) == 1:
            return matches[0]

    return candidate


def extract_judge_from_subject(subject):
    clean_subject = subject_without_forward_prefix(subject)

    patterns = [
        r"\bJudge\s+(.+?)(?=\s*[-,]|\s+(?:TEAMS?|Zoom|link|meeting|\d{1,2}/\d{1,2}/\d{2,4}|Monday|Tuesday|Wednesday|Thursday|Friday)\b|$)",
        r"^([A-Z][a-zA-Z'\-]+)\s*[-,]\s*#?\d",
    ]

    for pattern in patterns:
        match = re.search(pattern, clean_subject, re.I)
        if match:
            name = clean_judge_name(match.group(1))
            if is_reasonable_judge_name(name):
                return normalize_judge_name(name)

    return ""


def extract_judge_from_dol_header(text):
    patterns = [
        r"From:\s*([A-Z][A-Za-z'\-]+),\s*([A-Z][A-Za-z'\-]+)(?:\s+[A-Z]\.)?\s*\[DOL\]",
        r"To:\s*([A-Z][A-Za-z'\-]+),\s*([A-Z][A-Za-z'\-]+)(?:\s+[A-Z]\.)?\s*\[DOL\]",
        r"From:\s*([A-Z][A-Za-z'\-]+),\s*([A-Z][A-Za-z'\-]+).*?@dol\.nj\.gov",
        r"To:\s*([A-Z][A-Za-z'\-]+),\s*([A-Z][A-Za-z'\-]+).*?@dol\.nj\.gov",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, re.I):
            name = f"{match.group(2)} {match.group(1)}"
            if is_reasonable_judge_name(name):
                return normalize_judge_name(name)

    return ""


def extract_judge_from_email_address(text):
    pattern = r"\b([A-Z][A-Za-z]+)\.([A-Z][A-Za-z]+)@dol\.nj\.gov\b"

    for match in re.finditer(pattern, text, re.I):
        name = f"{match.group(1)} {match.group(2)}"
        if is_reasonable_judge_name(name):
            return normalize_judge_name(name)

    return ""


def extract_judge_from_signature(text):
    patterns = [
        r"\bHon\.?\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][A-Za-z'\-]+){0,3})\b",
        r"\bHonorable\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][A-Za-z'\-]+){0,3})\b",
        r"\bJudge\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][A-Za-z'\-]+){0,3})\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, re.I):
            name = clean_judge_name(match.group(1))
            if is_reasonable_judge_name(name):
                return normalize_judge_name(name)

    return ""


def extract_judge_from_dear_line(text):
    patterns = [
        r"\bDear\s+Judge\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+)?)\b",
        r"\bDear\s+Hon\.?\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+)?)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            name = clean_judge_name(match.group(1))
            if is_reasonable_judge_name(name):
                return normalize_judge_name(name)

    return ""


def extract_judge_from_common_list(text):
    text_norm = normalize_name_for_match(text)

    for judge in COMMON_JUDGES:
        judge_norm = normalize_name_for_match(judge)
        last = judge_last_name(judge)

        if judge_norm and judge_norm in text_norm:
            return judge

        if last and re.search(rf"\bjudge\s+{re.escape(last)}\b", text, re.I):
            return judge

    return ""


def extract_judge(subject, text, teams_link=""):
    context = get_context_around_teams_link(text, teams_link)

    for extractor in [
        lambda: extract_judge_from_dol_header(context),
        lambda: extract_judge_from_email_address(context),
        lambda: extract_judge_from_signature(context),
        lambda: extract_judge_from_subject(subject),
        lambda: extract_judge_from_dear_line(text),
        lambda: extract_judge_from_common_list(context),
        lambda: extract_judge_from_dol_header(text),
        lambda: extract_judge_from_email_address(text),
        lambda: extract_judge_from_signature(text),
        lambda: extract_judge_from_common_list(text),
    ]:
        value = extractor()
        if value:
            return normalize_judge_name(value)

    return ""


def parse_date_value(raw_date):
    try:
        parsed = date_parser.parse(raw_date, fuzzy=True)
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        return ""


def extract_date_from_subject(subject):
    clean_subject = subject_without_forward_prefix(subject)

    patterns = [
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
        r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b",
        r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, clean_subject, re.I)
        if match:
            value = parse_date_value(match.group(0))
            if value:
                return value

    return ""


def extract_date_from_body(text):
    patterns = [
        r"\b(?:listed|list|calendar of|for your list on|for your)\s*:?\s*(?:#\d+\s*[-]\s*)?((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})",
        r"\b((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})\b",
        r"\b(?:for|listing|listed|markings|calendar)\s+(?:the\s+)?(\d{1,2}/\d{1,2}/\d{2,4})\b",
        r"\b(\d{1,2}/\d{1,2}/\d{2,4})\s+(?:court\s+)?(?:list|listing|markings)\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, re.I):
            raw = match.group(1)
            value = parse_date_value(raw)
            if value:
                return value

    return ""


def normalize_time_string(raw):
    raw = raw.lower().strip()
    raw = raw.replace(".", "")
    raw = re.sub(r"\s+", " ", raw)

    match = re.fullmatch(r"(\d{1,2})(\d{2})", raw)
    if match:
        raw = f"{match.group(1)}:{match.group(2)} am"

    if re.fullmatch(r"\d{1,2}", raw):
        raw = f"{raw}:00 am"

    if re.fullmatch(r"\d{1,2}\s*(am|pm)", raw):
        raw = re.sub(r"(\d{1,2})\s*(am|pm)", r"\1:00 \2", raw)

    try:
        parsed = date_parser.parse(raw, fuzzy=True)
        return parsed.strftime("%I:%M %p")
    except Exception:
        return ""


def extract_time(text, teams_link=""):
    context = get_context_around_teams_link(text, teams_link)

    patterns = [
        r"\bTEAMS?\s+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b",
        r"\bconference\s+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b",
        r"\btestimony\s+(?:is\s+scheduled\s+)?(?:in-person\s+)?(?:at\s+)?(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)\b",
        r"\btestimony\s+at\s+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)\b",
        r"\bset\s+me\s+up\s+on\s+Monday\s+at\s+(\d{3,4})\b",
        r"\bon\s+Monday\s+at\s+(\d{3,4})\b",
        r"\bMonday\s+at\s+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)\b",
        r"\brecord\s+Monday\s+at\s+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b",
        r"\bI\s+start\s+at\s+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b",
        r"\b(?:start|begin)\s+at\s+(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b",
        r"\bat\s+(\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?))\b",
        r"\b(\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?))\b",
    ]

    for search_area in [context, text]:
        for pattern in patterns:
            match = re.search(pattern, search_area, re.I)
            if match:
                value = normalize_time_string(match.group(1))
                if value:
                    return value

    return ""


def remove_forwarded_sent_lines(text):
    return re.sub(
        r"Sent:\s*(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?.*?\d{1,2}:\d{2}\s*(AM|PM)",
        "",
        text,
        flags=re.I,
    )


def extract_date_time(subject, text, teams_link=""):
    court_date = extract_date_from_subject(subject)
    if not court_date:
        court_date = extract_date_from_body(text)

    court_time = extract_time(text, teams_link)

    cleaned = remove_forwarded_sent_lines(text)

    if not court_date:
        court_date = extract_date_from_body(cleaned)

    return court_date, court_time


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
    sender = message.get("from", {}).get("emailAddress", {}).get("address", "")

    body_obj = message.get("body", {}) or {}
    body_content = body_obj.get("content", "") or ""
    body_type = body_obj.get("contentType", "")

    html_links = []
    raw_html = ""

    if body_type.lower() == "html":
        raw_html = body_content
        body_text, html_links = html_to_text_with_links(body_content)
    else:
        body_text = normalize_text(body_content)

    full_text = normalize_text(
        subject + "\n" + body_text + "\n" + "\n".join(html_links) + "\n" + raw_html
    )

    teams_link = extract_teams_link(full_text, html_links, raw_html)
    judge = extract_judge(subject, full_text, teams_link)
    court_date, court_time = extract_date_time(subject, full_text, teams_link)

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
