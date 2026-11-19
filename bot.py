import json
import os
import re
import subprocess
import threading
import sqlite3
import time
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from lxml import html

FEED = "https://www.supremecommunity.com/restocks/us/"
NAME = "Appliqué Flannel Backpack"
SHOP = "https://us.supreme.com/products/hqjizbuf-64f49l"


def fetch_page(url):
    # A redirect can select another regional storefront. Accept only the US page.
    result = subprocess.run(
        ["curl", "--silent", "--show-error", "--max-time", "10", "--fail",
         "--write-out", "\\n%{http_code}", url],
        capture_output=True, check=True, timeout=12,
    )
    page, status = result.stdout.decode().rsplit("\n", 1)
    if status != "200":
        raise ValueError("Source page unavailable")
    return page


def feed_page(page):
    url = FEED if page == 1 else f"{FEED}{page}/"
    document = html.fromstring(fetch_page(url))
    rows = document.find_class("restock-item")
    if not rows:
        raise ValueError("Missing US feed entries")
    for row in rows:
        title = row.xpath(".//h3/text()")[0].strip()
        color = row.find_class("restock-color")[0].text_content().strip()
        stamp = row.xpath(".//time/@datetime")[0]
        yield datetime.fromisoformat(stamp.replace("Z", "+00:00")), (
            title == NAME and color == "Black" and not row.find_class("restock-badge-new")
        )


def scan(db):
    cutoff = db.execute("select cursor from state").fetchone()[0]
    latest = cutoff
    events = set()
    for page in range(1, 21):
        rows = list(feed_page(page))
        for date, matches in rows:
            stamp = int(date.timestamp())
            latest = max(latest, stamp)
            if matches and stamp >= cutoff:
                events.add(stamp)
        if min(int(date.timestamp()) for date, _ in rows) < cutoff or len(rows) < 8:
            break
    with db:
        for stamp in sorted(events):
            if db.execute("insert or ignore into events values(?)", (stamp,)).rowcount:
                record_signal(db, "SupremeCommunity", stamp)
        db.execute("update state set cursor=?", (latest,))


def record_signal(db, source, stamp):
    # Both sites publish different timestamps for the same restock.
    # Pair at most one report per source, choosing the nearest unmatched event.
    other = db.execute(
        "select id from signals where source != ? and paired=0 "
        "and abs(stamp-?)<=120 order by abs(stamp-?),id desc limit 1",
        (source, stamp, stamp),
    ).fetchone()
    if other:
        db.execute("update signals set paired=1 where id=?", other)
        return
    db.execute("insert into signals(source,stamp) values(?,?)", (source, stamp))
    msg = (f"US restock reported by {source}: {NAME} — Black\n"
           f"{datetime.fromtimestamp(stamp, timezone.utc):%Y-%m-%d %H:%M UTC}\n{SHOP}")
    db.execute("insert into queue(chat,body) select chat,? from subs where since<=?",
               (msg, stamp))


def official(db):
    page = fetch_page(SHOP)
    for match in re.finditer(r'\{\s*"id"\s*:\s*42734290272527\s*,', page):
        variant, _ = json.JSONDecoder().raw_decode(page[match.start():])
        available = variant.get("available")
        if variant.get("sku") == "FW26B12-BLK-OS" and type(available) is bool:
            break
    else:
        raise ValueError("US black variant availability missing")
    with db:
        db.execute("begin immediate")
        previous = db.execute("select available from official_state").fetchone()[0]
        if available and previous != 1:
            record_signal(db, "Supreme US", int(time.time()))
        db.execute("update official_state set available=?", (available,))
    if previous != available:
        print("Supreme US black variant available:", available, flush=True)


def telegram(method, **payload):
    url = "https://api.telegram.org/bot" + os.environ["TELEGRAM_BOT_TOKEN"] + "/" + method
    request = Request(url, json.dumps(payload).encode(), {"Content-Type": "application/json"})
    with urlopen(request, timeout=18) as response:
        return json.load(response)["result"]


def commands(db):
    offset = db.execute("select offset from state").fetchone()[0]
    for update in telegram("getUpdates", offset=offset, timeout=2, allowed_updates=["message"]):
        message = update.get("message", {})
        chat = message.get("chat", {})
        words = message.get("text", "").split()
        command = words[0] if words else ""
        with db:
            if chat.get("type") == "private" and command in ("/start", "/stop"):
                cid = chat["id"]
                if command == "/start":
                    db.execute("insert or ignore into subs values(?,?)", (cid, message["date"]))
                    reply = f"Subscribed to US restocks for the black {NAME}. Send /stop to unsubscribe."
                else:
                    db.execute("delete from subs where chat=?", (cid,))
                    db.execute("delete from queue where chat=?", (cid,))
                    reply = "Unsubscribed."
                db.execute("insert into queue(chat,body) values(?,?)", (cid, reply))
            db.execute("update state set offset=?", (update["update_id"] + 1,))


def deliver(db):
    for key, chat, body in db.execute("select id,chat,body from queue order by id limit 20").fetchall():
        try:
            telegram("sendMessage", chat_id=chat, text=body)
        except HTTPError as error:
            if error.code != 403:
                raise
            with db:
                db.execute("delete from subs where chat=?", (chat,))
                db.execute("delete from queue where chat=?", (chat,))
        with db:
            db.execute("delete from queue where id=?", (key,))
        time.sleep(.1)


def main():
    db = sqlite3.connect(os.getenv("DB_PATH", "bot.db"))
    db.executescript("""
    create table if not exists state(cursor integer, offset integer);
    insert into state select unixepoch(),0 where not exists(select 1 from state);
    create table if not exists events(stamp integer primary key);
    create table if not exists subs(chat integer primary key, since integer);
    create table if not exists queue(id integer primary key, chat integer, body text);
    """)
    db.executescript("""
    create table if not exists signals(id integer primary key, source text, stamp integer, paired integer default 0);
    create table if not exists official_state(available integer);
    insert into official_state select null where not exists(select 1 from official_state);
    """)
    db.close()
    jobs = [(official, 5), (scan, 5), (commands, 1), (deliver, 0.3)]
    threads = [threading.Thread(target=run_job, args=job) for job in jobs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def run_job(job, interval):
    db = sqlite3.connect(os.getenv("DB_PATH", "bot.db"), timeout=20)
    delay = interval
    first = True
    while True:
        try:
            job(db)
            if first:
                print(job.__name__, "first check OK", flush=True)
                first = False
            delay = interval
        except Exception as exc:
            db.rollback()
            delay = min(300, max(delay * 2, 15))
            if isinstance(exc, HTTPError) and exc.code == 429:
                try:
                    delay = max(delay, int(exc.headers.get("Retry-After", 60)))
                except ValueError:
                    delay = max(delay, 60)
            print(job.__name__, type(exc).__name__, getattr(exc, "code", ""), flush=True)
        time.sleep(delay)


if __name__ == "__main__":
    main()
