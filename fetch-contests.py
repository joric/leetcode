import csv, json, os, time, threading, requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

URL = "https://leetcode.com/graphql/"
HEADERS = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json", "Referer": "https://leetcode.com/"}

FILES = {"weekly": "weekly_contests.csv", "biweekly": "biweekly_contests.csv"}
FIELDS = ["contestSlug", "contestTitle", "date", "problemCount", "titleSlug",
          "questionFrontendId", "title", "difficulty", "acRate"]

csv_lock = threading.Lock()


def log(i, x):
    print("  " * i + str(x))


def gql(q, v=None):
    for n in range(3):
        try:
            r = requests.post(URL, headers=HEADERS, json={"query": q, "variables": v or {}}, timeout=30)
            r.raise_for_status()
            d = r.json()
            if "errors" in d:
                log(3, d["errors"])
                return None
            return d["data"]
        except Exception as e:
            log(3, f"retry {n + 1}: {e}")
            time.sleep(1)
    return None


ALL_CONTESTS = """
query{
 allContests{
  title
  titleSlug
  startTime
  duration
 }
}
"""

CONTEST_QUERY = """
query($slug:String!){
 contest(titleSlug:$slug){
  title
  startTime
  questions{
   titleSlug
  }
 }
}
"""

QUESTION_QUERY = """
query($slug:String!){
 question(titleSlug:$slug){
  questionFrontendId
  title
  titleSlug
  difficulty
  stats
 }
}
"""


def get_contests():
    log(0, "fetching contest list")
    d = gql(ALL_CONTESTS)
    if not d:
        raise RuntimeError("contest list failed")
    c = d["allContests"]
    c.sort(key=lambda x: x["startTime"])
    log(1, f"found {len(c)} contests")
    return c


def split_contests(c):
    w, b = [], []
    for x in c:
        (b if "biweekly" in x["title"].lower() else w).append(x)
    return w, b


def ensure_csv(p):
    if not os.path.exists(p):
        with open(p, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writeheader()


def load_progress(p):
    """Read the CSV once and return:
      saved  - set of (contestSlug, titleSlug) already recorded
      counts - contestSlug -> problemCount, as last recorded in the CSV
    counts lets us know a contest is fully saved WITHOUT hitting the network,
    since a past contest's problem set never changes once it's over."""
    saved = set()
    counts = {}
    dupes = 0
    if not os.path.exists(p):
        return saved, counts
    with open(p, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            slug, t = r.get("contestSlug", ""), r.get("titleSlug", "")
            k = (slug, t)
            if k in saved:
                dupes += 1
            saved.add(k)
            pc = r.get("problemCount")
            if slug and pc:
                counts[slug] = int(pc)
    if dupes:
        log(1, f"warning: {dupes} duplicate rows found in {p} (ignored)")
    return saved, counts


def append_row(p, row):
    # csv_lock serializes writes so concurrent worker threads can't interleave rows
    with csv_lock:
        with open(p, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writerow(row)


def fetch_contest(slug):
    d = gql(CONTEST_QUERY, {"slug": slug})
    if not d:
        log(2, f"could not fetch contest data for {slug} (network/graphql error)")
        return None
    c = d.get("contest")
    if not c:
        log(2, f"contest not found: {slug}")
        return None
    c["_slug"] = slug
    return c


def fetch_question(slug):
    d = gql(QUESTION_QUERY, {"slug": slug})
    return d.get("question") if d else None


def process(q):
    try:
        stats = json.loads(q.get("stats") or "{}")
    except Exception:
        stats = {}
    return {
        "titleSlug": q.get("titleSlug", ""),
        "questionFrontendId": q.get("questionFrontendId", ""),
        "title": q.get("title", ""),
        "difficulty": q.get("difficulty", ""),
        "acRate": str(stats.get("acRate", "")).replace("%", ""),
    }


def download(contest, qinfo):
    q = fetch_question(qinfo["titleSlug"])
    if not q:
        return None
    row = process(q)
    row.update({
        "contestSlug": contest["_slug"],
        "contestTitle": contest["title"],
        "date": datetime.fromtimestamp(contest["startTime"], timezone.utc).strftime("%Y-%m-%d"),
        "problemCount": len(contest["questions"]),
    })
    return row


def update(contests, path, kind):
    ensure_csv(path)
    saved, counts = load_progress(path)

    log(0, kind.upper())
    log(1, f"saved problems: {len(saved)}")

    skipped = 0
    for i, entry in enumerate(contests, 1):
        slug = entry["titleSlug"]
        existing = {q for c, q in saved if c == slug}

        # Already fully saved? A past contest's problems never change, so we
        # can trust the CSV and skip the network call entirely.
        if slug in counts and len(existing) >= counts[slug]:
            skipped += 1
            continue

        log(1, f"[{i}/{len(contests)}] {slug}")

        contest = fetch_contest(slug)
        if not contest:
            continue

        questions = contest.get("questions", [])
        total = len(questions)
        missing = [q for q in questions if q["titleSlug"] not in existing]

        if not missing:
            log(2, f"complete {total}/{total}")
            continue

        log(2, f"{contest['title']} missing {len(missing)}/{total}")

        done_count = total - len(missing)
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(download, contest, q) for q in missing]

            for f in as_completed(futures):
                try:
                    row = f.result()
                except Exception as e:
                    log(3, f"worker error: {e}")
                    continue

                if not row:
                    continue

                append_row(path, row)
                saved.add((row["contestSlug"], row["titleSlug"]))
                done_count += 1
                log(3, f"saved {row['titleSlug']} ({done_count}/{total})")

    if skipped:
        log(1, f"skipped {skipped} already-complete contests (no network call)")


def main():
    print("cwd:", os.getcwd())

    contests = get_contests()
    weekly, biweekly = split_contests(contests)

    log(0, f"weekly: {len(weekly)}")
    log(0, f"biweekly: {len(biweekly)}")

    update(weekly, FILES["weekly"], "weekly")
    update(biweekly, FILES["biweekly"], "biweekly")

    print("finished")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted. CSV already flushed.")
