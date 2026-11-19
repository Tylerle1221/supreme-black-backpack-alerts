import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from lxml import html

FEED = "https://www.supremecommunity.com/restocks/us/"
NAME = "Appliqué Flannel Backpack"
SHOP = "https://us.supreme.com/collections/bags"


def feed_page(page):
    url = FEED if page == 1 else f"{FEED}{page}/"
    with urlopen(url, timeout=12) as response:
        document = html.fromstring(response.read().decode("utf-8"))
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
                msg = (f"US restock reported: {NAME} — Black\n"
                       f"{datetime.fromtimestamp(stamp, timezone.utc):%Y-%m-%d %H:%M UTC}\n{SHOP}")
                db.execute("insert into queue(chat,body) select chat,? from subs where since<=?",
                           (msg, stamp))
        db.execute("update state set cursor=?", (latest,))


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
    jobs = [(scan, 30), (commands, 1), (deliver, 1)]
    next_run = {}
    while True:
        for job, interval in jobs:
            if time.monotonic() < next_run.get(job, 0):
                continue
            delay = interval
            try:
                job(db)
            except Exception as exc:
                print(job.__name__, type(exc).__name__, getattr(exc, "code", ""), flush=True)
                delay = max(interval, 30)
                if isinstance(exc, HTTPError) and exc.code == 429:
                    try:
                        delay = max(delay, int(exc.headers.get("Retry-After", 0)),
                                    json.loads(exc.read()).get("parameters", {}).get("retry_after", 0))
                    except (ValueError, TypeError):
                        delay = max(delay, 60)
            next_run[job] = time.monotonic() + delay
        time.sleep(.2)


if __name__ == "__main__":
    main()
