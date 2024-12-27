import datetime
import re
import sqlite3
from pathlib import Path
from textwrap import dedent
from typing import Self

import atproto
import attr
import bs4
import dateutil.tz
import mastodon
import requests
import tomllib
import typer
from atproto_client.models.app.bsky.embed import external as bsky_card_model


@attr.frozen()
class PendingToot:
    event_id: str
    content: str


@attr.frozen()
class PendingSkeet:
    event_id: str
    content: str
    link: str


@attr.define()
class EventInfo:
    event_date: str | None
    facility: str | None
    city: str | None
    state: str | None
    headline: str
    content: str

    @property
    def location(self) -> str:
        if self.facility and self.state:
            return f"{self.facility} ({self.state})"
        if self.facility:
            return self.facility
        if self.city and self.state:
            return f"{self.city}, {self.state}"
        if self.city:
            return self.city
        if self.state:
            return self.state
        return "Location unknown"

    def format(self):
        return (
            dedent(
                f"""\
            {self.headline}
            {self.location}, {self.event_date or 'Date unknown'}
        """
            )
            + self.content
        )


schema = """
CREATE TABLE IF NOT EXISTS toots (
    event_id TEXT UNIQUE NOT NULL,
    timestamp TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL,
    content TEXT NOT NULL,
    pending INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS skeets (
    event_id TEXT UNIQUE NOT NULL,
    timestamp TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL,
    content TEXT NOT NULL,
    link TEXT NOT NULL,
    pending INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS urls (
    url TEXT NOT NULL UNIQUE,
    visited TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
"""


@attr.define()
class BotStore:
    connection: sqlite3.Connection

    def __attrs_post_init__(self):
        with self.connection:
            self.connection.executescript(schema)

    @classmethod
    def from_path(cls, path) -> Self:
        conn = sqlite3.connect(path)
        return cls(conn)

    def next_toot(self) -> PendingToot | None:
        with self.connection:
            cursor = self.connection.execute(
                """\
                SELECT event_id, content
                FROM toots
                WHERE pending
                ORDER BY timestamp ASC
                LIMIT 1
            """
            )
            row = cursor.fetchone()
        if not row:
            return None
        return PendingToot(
            event_id=row[0],
            content=row[1],
        )

    def next_skeet(self) -> PendingSkeet | None:
        with self.connection:
            cursor = self.connection.execute(
                """\
                SELECT event_id, content, link
                FROM skeets
                WHERE pending
                ORDER BY timestamp ASC
                LIMIT 1
            """
            )
            row = cursor.fetchone()
        if not row:
            return None
        return PendingSkeet(
            event_id=row[0],
            content=row[1],
            link=row[2],
        )

    def last_visit(self, url: str) -> datetime.datetime | None:
        with self.connection:
            cursor = self.connection.execute(
                """\
                SELECT visited
                FROM urls
                WHERE url = ?
            """,
                (url,),
            )
            row = cursor.fetchone()
        if not row:
            return None
        return datetime.datetime.fromisoformat(row[0])

    def record_visit(self, urls: list[str]) -> None:
        with self.connection:
            self.connection.executemany(
                """\
                INSERT OR REPLACE INTO urls
                VALUES(:url, CURRENT_TIMESTAMP)""",
                [dict(url=url) for url in urls],
            )

    def save_toots(self, toots: list[PendingToot]):
        with self.connection:
            self.connection.executemany(
                """\
                INSERT OR IGNORE INTO toots
                (event_id, content, pending)
                VALUES(:event_id, :content, TRUE)
                """,
                [dict(event_id=toot.event_id, content=toot.content) for toot in toots],
            )

    def save_skeets(self, skeets: list[PendingSkeet]):
        with self.connection:
            self.connection.executemany(
                """\
                INSERT OR IGNORE INTO skeets
                (event_id, content, link, pending)
                VALUES(:event_id, :content, :link, TRUE)
                """,
                [
                    dict(
                        event_id=skeet.event_id, content=skeet.content, link=skeet.link
                    )
                    for skeet in skeets
                ],
            )

    def record_toot(self, toot: PendingToot):
        with self.connection:
            self.connection.execute(
                """\
                UPDATE toots
                SET pending = FALSE
                WHERE event_id = ?
            """,
                (toot.event_id,),
            )

    def record_skeet(self, skeet: PendingSkeet):
        with self.connection:
            self.connection.execute(
                """\
                UPDATE skeets
                SET pending = FALSE
                WHERE event_id = ?
            """,
                (skeet.event_id,),
            )


def extract(table: bs4.Tag) -> EventInfo:
    """Extract an EventInfo from an event div from the NRC website."""
    facility = None
    if m := re.search(r"Facility: (.*?)\s+\w+:", table.text, re.MULTILINE | re.DOTALL):
        facility = m[1]

    city = None
    if m := re.search(r"City: (.*?)\s+\w+:", table.text, re.MULTILINE | re.DOTALL):
        city = m[1]

    state = None
    if m := re.search("State: ([A-Z]{2})", table.text):
        state = m[1]

    event_date = None
    if m := re.search(r"Event Date: ([\d/]+)", table.text, re.MULTILINE | re.DOTALL):
        event_date = m[1]

    event_text = table.find_next(string="Event Text").find_next("div").text.strip()  # type: ignore
    headline, summary = event_text.split("\r\n", maxsplit=1)
    headline = headline.strip().split(" - ", maxsplit=1)[-1]
    summary = re.split(r"via .*?:", summary, maxsplit=1)[-1].strip()

    return EventInfo(
        event_date=event_date,
        facility=facility,
        city=city,
        state=state,
        headline=headline,
        content=summary,
    )


def format_toot(text: str, url: str, maxlen: int = 500) -> str:
    url_len = 23
    max_text_len = maxlen - url_len - 3
    text = text.replace("\r\n", "\n")
    ellipsis = "…" if len(text) > max_text_len else ""
    return "".join([text[:max_text_len].strip(), ellipsis, "\n", url])


def truncate(text: str, maxlen: int) -> str:
    if len(text) > maxlen:
        text = text[: maxlen - 1] + "…"
    return text


def page_as_toots(content: str, url: str) -> list[PendingToot]:
    html = bs4.BeautifulSoup(content, features="html.parser")
    events = html.find_all("div", id=re.compile(r"en\d+"))
    toots = []
    for e in events:
        toots.append(
            PendingToot(
                event_id=e.attrs["id"],
                content=format_toot(extract(e).format(), f"{url}#{e.attrs['id']}"),
            )
        )
    return toots


def page_as_skeets(content: str, url: str) -> list[PendingSkeet]:
    html = bs4.BeautifulSoup(content, features="html.parser")
    events = html.find_all("div", id=re.compile(r"en\d+"))
    skeets = []
    for e in events:
        skeets.append(
            PendingSkeet(
                event_id=e.attrs["id"],
                content=truncate(extract(e).format(), 300),
                link=f"{url}#{e.attrs['id']}",
            )
        )
    return skeets


app = typer.Typer()

todays_date = datetime.datetime.now(dateutil.tz.gettz("America/Eastern")).strftime(
    "%Y%m%d"
)


@app.command()
def scrape(database: Path, ymd: str = todays_date):
    assert len(ymd) == 8
    assert ymd.isnumeric()
    store = BotStore.from_path(database)
    year = ymd[:4]
    url = f"https://www.nrc.gov/reading-rm/doc-collections/event-status/event/{year}/{ymd}en.html"
    if store.last_visit(url):
        return
    response = requests.get(url)
    if response.status_code != 200:
        print(response.status_code)
        return
    toots = page_as_toots(response.text, url)
    store.save_toots(toots)
    skeets = page_as_skeets(response.text, url)
    store.save_skeets(skeets)
    store.record_visit([url])


@app.command()
def toot(
    database: Path,
    dry_run: bool = False,
    secrets: Path = Path("secrets.toml"),
    post_to_mastodon: bool = typer.Option(True, "--mastodon/--no-mastodon"),
    post_to_bluesky: bool = typer.Option(True, "--bluesky/--no-bluesky"),
):
    store = BotStore.from_path(database)
    secrets_dict = tomllib.loads(secrets.read_text())

    toot = store.next_toot()
    if toot and post_to_mastodon:
        client = mastodon.Mastodon(**secrets_dict["mastodon"])
        if dry_run:
            print(toot.content)
        else:
            receipt = client.status_post(toot.content)
            store.record_toot(toot)
            print(receipt["url"])

    skeet = store.next_skeet()
    if skeet and post_to_bluesky:
        bsky = atproto.Client()
        bsky.login(
            secrets_dict["bluesky"]["handle"],
            secrets_dict["bluesky"]["password"],
        )
        card = bsky_card_model.Main(
            external=bsky_card_model.External(
                uri=skeet.link,
                description="US Nuclear Regulatory Commission",
                title="Event Notifications",
            )
        )
        if dry_run:
            print(skeet.content)
            print(card)
        else:
            receipt = bsky.send_post(text=skeet.content, embed=card, langs=["en-US"])
            store.record_skeet(skeet)
            print(receipt.uri)


if __name__ == "__main__":
    app()
