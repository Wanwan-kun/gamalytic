#!/usr/bin/env python3
"""
gamalytic.py

(обновлённый) Fetch from Gamalytic API and compute stats (combined script)
with adaptive token-bucket rate limiter and Retry-After handling.

Добавлено:
 - извлечение поля tags (список тегов) из API-ответа
 - сохранение tags в выходной JSON/CSV (в CSV теги разделяются "|")
 - при загрузке CSV/JSON нормализуются теги в список
 - вычисление Top 10 tags of all games и вывод блока сразу после Top {top_n} games by revenue

Новые флаги (rate limitting):
  --rps                начальная скорость, запросов в секунду (default 1.0)
  --rps-min            минимальная скорость (default 0.1)
  --rps-max            максимальная скорость (default 5.0)
  --rps-increase       additive increase step при успехе (default 0.05)
  --rps-decrease-factor multiplicative decrease factor при 429 (default 0.5)
"""
import asyncio
import aiohttp
import argparse
import csv
import json
import math
import os
import random
import statistics
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter

API_URL_TEMPLATE = "https://api.gamalytic.com/game/{}"

FIELD_CANDIDATES = {
    "name": ["name", "title", "game", "gameName", "app_name", "appName"],
    "players": ["players", "players_total", "playersTotal", "players_total_estimated"],
    "owners": ["owners", "owners_total", "ownersTotal", "owners_estimated"],
    "copiesSold": ["copiesSold", "copies_sold", "copies_sold_estimated", "copiesSoldEstimate", "copies"],
    "revenue": ["revenue", "gross_revenue", "grossRevenue", "totalRevenue", "total_revenue"],
    "price": ["price", "current_price", "price_usd", "price_formatted", "price_current", "price_final"],
    "reviewsSteam": ["reviews", "reviewsSteam", "steam_reviews", "review_count", "reviews_count", "steam_review_count"],
    "reviewScore": ["reviewScore", "score", "review_score", "rating", "reviewRating"],
    "avgPlaytime": ["avgPlaytime", "average_playtime", "avg_playtime", "avg_playtime_hours", "average_playtime_hours", "playtime", "average_playtime_minutes"]
}

TAG_CANDIDATES = ["tags", "genres", "categories", "tags_list", "game_tags", "labels"]

# ----------------- Adaptive token bucket -----------------
class AdaptiveTokenBucket:
    """
    Simple adaptive token bucket:
      - rate = tokens added per second (float)
      - tokens are allowed to accumulate up to burst (set to max(1, rate*2))
      - consume(): wait until at least 1 token is available, then consume it
      - on_success(): small additive increase to rate up to max_rate
      - on_429(retry_after): multiplicative decrease and optional cooldown
    """
    def __init__(self, rate: float = 1.0, min_rate: float = 0.1, max_rate: float = 5.0,
                 increase: float = 0.05, decrease_factor: float = 0.5):
        self._rate = float(rate)
        self.min_rate = float(min_rate)
        self.max_rate = float(max_rate)
        self.increase = float(increase)
        self.decrease_factor = float(decrease_factor)
        self._tokens = 0.0
        self._last = time.monotonic()
        self._lock = asyncio.Lock()
        self._cooldown_until = 0.0  # if > now, callers should wait
        # max burst set to a couple seconds worth of tokens
        self._burst_factor = 2.0

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed <= 0:
            return
        add = elapsed * self._rate
        burst = max(1.0, self._rate * self._burst_factor)
        self._tokens = min(burst, self._tokens + add)
        self._last = now

    async def consume(self):
        """
        Wait until token available and cooldown passed, then consume 1 token.
        """
        while True:
            async with self._lock:
                # refill tokens based on elapsed time
                self._refill()
                now = time.monotonic()
                if self._cooldown_until > now:
                    wait = self._cooldown_until - now
                    # release lock and sleep
                    # small jitter to avoid synchronized bursts
                    await asyncio.sleep(wait + random.uniform(0, 0.2))
                    continue
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # not enough tokens -> calculate approximate wait time for 1 token
                needed = 1.0 - self._tokens
                if self._rate <= 0:
                    wait_time = 1.0
                else:
                    wait_time = needed / self._rate
            # outside lock
            await asyncio.sleep(wait_time + random.uniform(0, 0.02))

    def on_success(self):
        """
        Called after a successful request to slowly increase the rate.
        Additive increase up to max_rate.
        """
        async def _inc():
            async with self._lock:
                self._rate = min(self.max_rate, self._rate + self.increase)
        # schedule quickly (don't await to not block caller)
        try:
            asyncio.get_event_loop().create_task(_inc())
        except Exception:
            # fallback synchronous (shouldn't usually happen)
            self._rate = min(self.max_rate, self._rate + self.increase)

    def on_429(self, retry_after: Optional[float] = None):
        """
        Called when server returned 429.
        - Multiplicative decrease of rate.
        - Set cooldown_until = now + retry_after (if provided) or a small default.
        - Drain tokens to avoid immediate retries.
        """
        async def _do():
            async with self._lock:
                new_rate = max(self.min_rate, self._rate * self.decrease_factor)
                self._rate = new_rate
                # drain tokens
                self._tokens = 0.0
                now = time.monotonic()
                cooldown = 0.0
                if retry_after and retry_after > 0:
                    cooldown = float(retry_after)
                else:
                    cooldown = max(1.0, 1.0 / max(0.1, self._rate))
                self._cooldown_until = now + cooldown
        try:
            asyncio.get_event_loop().create_task(_do())
        except Exception:
            # fallback synchronous
            new_rate = max(self.min_rate, self._rate * self.decrease_factor)
            self._rate = new_rate
            self._tokens = 0.0
            now = time.monotonic()
            cooldown = retry_after if retry_after else max(1.0, 1.0 / max(0.1, self._rate))
            self._cooldown_until = now + cooldown

    def get_rate(self):
        return self._rate

# ----------------- Helpers (existing) -----------------
def coerce_number(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)) and not (isinstance(val, float) and math.isnan(val)):
        return float(val)
    s = str(val).strip()
    if s == "" or s.lower() in ("n/a", "none", "-", "null"):
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()
    s_clean = re.sub(r"[^\d\.,eE\+\-]", "", s)
    if s_clean == "":
        return None
    s = s_clean
    try:
        if "," in s and "." in s:
            s = s.replace(",", "")
        elif s.count(",") > 1 and "." not in s:
            s = s.replace(",", "")
        elif "," in s and "." not in s:
            if s.count(",") == 1 and re.match(r"^\d+,\d+$", s):
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        num = float(s)
        if neg:
            num = -num
        return num
    except Exception:
        return None


def try_extract_field(obj: Any, candidates: List[str]) -> Optional[Any]:
    if obj is None:
        return None
    if isinstance(obj, dict):
        for c in candidates:
            if c in obj and obj[c] is not None:
                return obj[c]
        if "data" in obj and isinstance(obj["data"], dict):
            for c in candidates:
                if c in obj["data"] and obj["data"][c] is not None:
                    return obj["data"][c]
        for v in obj.values():
            if isinstance(v, dict):
                for c in candidates:
                    if c in v and v[c] is not None:
                        return v[c]
    if isinstance(obj, str) and (obj.strip().startswith("{") or obj.strip().startswith("[")):
        try:
            parsed = json.loads(obj)
            return try_extract_field(parsed, candidates)
        except Exception:
            pass
    return None


def normalize_tags(val: Any) -> List[str]:
    """Normalize tags into a list of trimmed strings."""
    if val is None:
        return []
    if isinstance(val, list):
        out = []
        for it in val:
            if it is None:
                continue
            s = str(it).strip()
            if s:
                out.append(s)
        return out
    if isinstance(val, dict):
        # sometimes tags are a dict of {id: name} or similar
        out = []
        for v in val.values():
            if v is None:
                continue
            s = str(v).strip()
            if s:
                out.append(s)
        return out
    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return []
        # try parse JSON array string
        if s.startswith("[") or s.startswith("{"):
            try:
                parsed = json.loads(s)
                return normalize_tags(parsed)
            except Exception:
                pass
        # split on common separators
        parts = re.split(r"[,\|;]+", s)
        parts = [p.strip() for p in parts if p.strip()]
        return parts
    # fallback
    s = str(val).strip()
    return [s] if s else []


def format_money_int(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    try:
        n = int(round(v))
    except Exception:
        return "N/A"
    sign = "-" if n < 0 else ""
    s = f"{abs(n):,}".replace(",", " ")
    return f"{sign}${s}"


def format_price(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    try:
        n = float(v)
    except Exception:
        return "N/A"
    sign = "-" if n < 0 else ""
    integer = int(abs(n))
    frac = abs(n) - integer
    int_part = f"{integer:,}".replace(",", " ")
    frac_part = f"{frac:.2f}"[1:]
    return f"{sign}${int_part}{frac_part}"


def format_int_spaces(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    try:
        n = int(round(v))
    except Exception:
        return "N/A"
    sign = "-" if n < 0 else ""
    s = f"{abs(n):,}".replace(",", " ")
    return f"{sign}{s}"


def format_score(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    try:
        return f"{float(v):.2f}"
    except Exception:
        return str(v)


def playtime_hours_to_hm(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    try:
        hours_float = float(v)
    except Exception:
        return "N/A"
    if hours_float < 0:
        return "N/A"
    total_minutes = int(round(hours_float * 60))
    hrs = total_minutes // 60
    mins = total_minutes % 60
    if hrs > 0 and mins > 0:
        return f"{hrs}h {mins}min"
    if hrs > 0:
        return f"{hrs}h"
    return f"{mins}min"

# ----------------- Fetch logic (modified to use token bucket) -----------------
async def fetch_one(session: aiohttp.ClientSession, appid: str, api_key: Optional[str],
                    token_bucket: AdaptiveTokenBucket,
                    timeout: int = 20, max_attempts: int = 5, base_backoff: float = 0.8) -> Dict[str, Any]:
    """
    Each attempt will:
      - await token_bucket.consume() before sending request
      - handle 429 by reading Retry-After, calling token_bucket.on_429(retry_after) and waiting
      - on success call token_bucket.on_success()
    """
    url = API_URL_TEMPLATE.format(appid)
    attempt = 0
    last_exc = None

    auth_modes = ["bearer", "x-api-key", "query"]

    while attempt < max_attempts:
        attempt += 1
        headers = {"Accept": "application/json", "User-Agent": "gamalytic-full/1.0"}
        params: Dict[str, Any] = {}
        if api_key:
            mode = auth_modes[min(attempt - 1, len(auth_modes) - 1)]
            if mode == "bearer":
                headers["Authorization"] = f"Bearer {api_key}"
            elif mode == "x-api-key":
                headers["X-API-Key"] = api_key
            else:
                params["api_key"] = api_key

        # Wait for a token (global rate limiter)
        await token_bucket.consume()

        try:
            async with session.get(url, headers=headers, params=params, timeout=timeout) as resp:
                status = resp.status
                text = await resp.text()
                if 200 <= status < 300:
                    # success -> notify token bucket to gently increase rate
                    token_bucket.on_success()
                if status == 404:
                    return {"appid": appid, "error": "not_found", "status": 404}
                if status == 401:
                    last_exc = f"401 unauthorized (attempt {attempt})"
                    await asyncio.sleep(base_backoff * (2 ** (attempt - 1)) + random.random() * 0.3)
                    continue
                if status == 429:
                    # read Retry-After
                    ra = resp.headers.get("Retry-After")
                    retry_after = None
                    if ra:
                        try:
                            retry_after = float(ra)
                        except Exception:
                            # sometimes Retry-After is HTTP-date; fallback to default small pause
                            retry_after = None
                    # inform token bucket to reduce rate and set cooldown
                    token_bucket.on_429(retry_after)
                    # sleep for Retry-After (or small backoff) then retry
                    wait = retry_after if retry_after and retry_after > 0 else (base_backoff * (2 ** (attempt - 1)) + random.random())
                    await asyncio.sleep(wait)
                    continue
                if status >= 500:
                    await asyncio.sleep(base_backoff * (2 ** (attempt - 1)) + random.random())
                    continue

                try:
                    data = json.loads(text) if text else {}
                except Exception:
                    return {"appid": appid, "error": "invalid_json", "raw_text": text}

                game_obj = data.get("data") if isinstance(data, dict) and "data" in data else data

                out: Dict[str, Any] = {"appid": str(appid)}
                out["name"] = try_extract_field(game_obj, FIELD_CANDIDATES["name"]) or ""
                out["players"] = coerce_number(try_extract_field(game_obj, FIELD_CANDIDATES["players"]))
                out["owners"] = coerce_number(try_extract_field(game_obj, FIELD_CANDIDATES["owners"]))
                out["copiesSold"] = coerce_number(try_extract_field(game_obj, FIELD_CANDIDATES["copiesSold"]))
                out["revenue"] = coerce_number(try_extract_field(game_obj, FIELD_CANDIDATES["revenue"]))
                out["price"] = coerce_number(try_extract_field(game_obj, FIELD_CANDIDATES["price"]))
                out["reviewsSteam"] = coerce_number(try_extract_field(game_obj, FIELD_CANDIDATES["reviewsSteam"]))
                out["reviewScore"] = coerce_number(try_extract_field(game_obj, FIELD_CANDIDATES["reviewScore"]))
                out["avgPlaytime"] = coerce_number(try_extract_field(game_obj, FIELD_CANDIDATES["avgPlaytime"]))

                # extract tags (normalize to list of strings)
                tags_raw = try_extract_field(game_obj, TAG_CANDIDATES)
                out["tags"] = normalize_tags(tags_raw)

                out["raw"] = game_obj
                return out
        except asyncio.TimeoutError:
            last_exc = "timeout"
            await asyncio.sleep(base_backoff * (2 ** (attempt - 1)) + random.random())
            continue
        except aiohttp.ClientError as e:
            last_exc = str(e)
            await asyncio.sleep(base_backoff * (2 ** (attempt - 1)) + random.random())
            continue
        except Exception as e:
            last_exc = str(e)
            await asyncio.sleep(base_backoff * (2 ** (attempt - 1)) + random.random())
            continue

    return {"appid": str(appid), "error": "failed", "reason": last_exc}


async def fetch_all(appids: List[str], api_key: Optional[str], concurrency: int, timeout: int, delay: float,
                    token_bucket: AdaptiveTokenBucket) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    semaphore = asyncio.Semaphore(concurrency)
    conn = aiohttp.TCPConnector(limit=concurrency * 2, ttl_dns_cache=300)
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(connector=conn, timeout=client_timeout) as session:
        tasks = []
        total = len(appids)
        for idx, aid in enumerate(appids, start=1):
            async def bounded_fetch(aid_local=aid, idx_local=idx):
                async with semaphore:
                    if delay and delay > 0:
                        await asyncio.sleep(delay)
                    print(f"[{idx_local}/{total}] fetching {aid_local} ...", flush=True)
                    r = await fetch_one(session, aid_local, api_key, token_bucket, timeout=timeout)
                    status = "ok" if "error" not in r else f"err:{r.get('error')}"
                    print(f"[{idx_local}/{total}] {aid_local} -> {status}", flush=True)
                    results.append(r)
            tasks.append(asyncio.create_task(bounded_fetch()))
        await asyncio.gather(*tasks)
    order = {aid: i for i, aid in enumerate(appids)}
    results_sorted = sorted(results, key=lambda r: order.get(str(r.get("appid")), 999999))
    return results_sorted

# ----------------- IO & Stats (same as before, but with tags handling) -----------------
def save_data(results: List[Dict[str, Any]], outpath: str, fmt: str = "csv"):
    outpath = Path(outpath)
    if fmt == "json" or outpath.suffix.lower() == ".json":
        with outpath.open("w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"Saved JSON to {outpath}")
        return
    # CSV path
    fieldnames = ["appid", "name", "players", "owners", "copiesSold", "revenue", "price", "reviewsSteam", "reviewScore", "avgPlaytime", "tags"]
    with outpath.open("w", encoding="utf-8", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in fieldnames}
            tags = r.get("tags")
            if isinstance(tags, list):
                row["tags"] = "|".join(tags)
            elif tags is None:
                row["tags"] = ""
            else:
                row["tags"] = str(tags)
            writer.writerow(row)
    print(f"Saved CSV to {outpath}")


def load_data(path: str, fmt: str = "csv") -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    if fmt == "json" or p.suffix.lower() == ".json":
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            if "data" in data and isinstance(data["data"], list):
                items = data["data"]
            else:
                items = [data]
        else:
            items = [data]
    else:
        items = []
        with p.open("r", encoding="utf-8", errors="replace") as f:
            sample = f.read(8192)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample)
            except Exception:
                dialect = csv.excel
            reader = csv.DictReader(f, dialect=dialect)
            for row in reader:
                items.append(row)
    numeric_keys = ["players", "owners", "copiesSold", "revenue", "price", "reviewsSteam", "reviewScore", "avgPlaytime"]
    coerced = []
    for it in items:
        if not isinstance(it, dict):
            coerced.append(it)
            continue
        out = dict(it)
        for k in numeric_keys:
            if k in out:
                out[k] = coerce_number(out[k])
            else:
                out[k] = None
        # normalize tags field
        if "tags" in out:
            t = out["tags"]
            if t is None:
                out["tags"] = []
            elif isinstance(t, list):
                out["tags"] = [str(x).strip() for x in t if x is not None and str(x).strip()]
            elif isinstance(t, str):
                s = t.strip()
                if s == "":
                    out["tags"] = []
                elif "|" in s:
                    out["tags"] = [x.strip() for x in s.split("|") if x.strip()]
                elif "," in s:
                    out["tags"] = [x.strip() for x in s.split(",") if x.strip()]
                else:
                    out["tags"] = [s]
            else:
                out["tags"] = [str(t).strip()]
        else:
            out["tags"] = []
        coerced.append(out)
    return coerced


def compute_stats_list(values: List[Optional[float]]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], int]:
    clean = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not clean:
        return None, None, None, None, 0
    mn = min(clean)
    mx = max(clean)
    mean = statistics.mean(clean)
    median = statistics.median(clean)
    return mn, mx, mean, median, len(clean)


def build_results_text(results: List[Dict[str, Any]], top_n: int = 5) -> str:
    revenues = [r.get("revenue") for r in results]
    copies = [r.get("copiesSold") for r in results]
    prices = [r.get("price") for r in results]

    rev_min, rev_max, rev_mean, rev_median, rev_count = compute_stats_list(revenues)
    cop_min, cop_max, cop_mean, cop_median, cop_count = compute_stats_list(copies)
    pr_min, pr_max, pr_mean, pr_median, pr_count = compute_stats_list(prices)

    lines: List[str] = []

    lines.append("CopiesSold statistics:")
    if cop_count == 0:
        lines.append("  (no numeric copiesSold values found)")
    else:
        lines.append(f"  Game count: {cop_count}")
        lines.append(f"  Averge: {format_int_spaces(cop_mean)}")
        lines.append(f"  Median: {format_int_spaces(cop_median)}")
        lines.append(f"  Min   : {format_int_spaces(cop_min)}")
        lines.append(f"  Max   : {format_int_spaces(cop_max)}")
    lines.append("")

    lines.append("Revenue statistics:")
    if rev_count == 0:
        lines.append("  (no numeric revenue values found)")
    else:
        lines.append(f"  Game count: {rev_count}")
        lines.append(f"  Averge: {format_money_int(rev_mean)}")
        lines.append(f"  Median: {format_money_int(rev_median)}")
        lines.append(f"  Min   : {format_money_int(rev_min)}")
        lines.append(f"  Max   : {format_money_int(rev_max)}")
    lines.append("")

    lines.append("Price statistics:")
    if pr_count == 0:
        lines.append("  (no numeric price values found)")
    else:
        def fmt_p(x): return format_price(x)
        lines.append(f"  Game count: {pr_count}")
        lines.append(f"  Averge: {fmt_p(pr_mean)}")
        lines.append(f"  Median: {fmt_p(pr_median)}")
        lines.append(f"  Min   : {fmt_p(pr_min)}")
        lines.append(f"  Max   : {fmt_p(pr_max)}")
    lines.append("")

    lines.append(f"Top {top_n} games by revenue:")
    entries = [r for r in results if r.get("revenue") is not None]
    entries_sorted = sorted(entries, key=lambda x: x.get("revenue", -1), reverse=True)[:top_n]
    if not entries_sorted:
        lines.append("  (no entries)")
        lines.append("")
        return "\n".join(lines)

    rows = []
    for i, e in enumerate(entries_sorted, start=1):
        name = e.get("name") or ""
        price_s = format_price(e.get("price"))
        revenue_s = format_money_int(e.get("revenue"))
        copies_s = format_int_spaces(e.get("copiesSold"))
        reviews_s = format_int_spaces(e.get("reviewsSteam"))
        score_s = format_score(e.get("reviewScore"))
        play_s = playtime_hours_to_hm(e.get("avgPlaytime"))
        rows.append({
            "rank": str(i),
            "name": name,
            "price": price_s,
            "revenue": revenue_s,
            "copies": copies_s,
            "reviews": reviews_s,
            "score": score_s,
            "play": play_s
        })

    max_name_len = min(40, max(len(r["name"]) for r in rows))
    rank_w = max(4, max(len(r["rank"]) for r in rows))
    price_w = max(5, max(len(r["price"]) for r in rows))
    revenue_w = max(7, max(len(r["revenue"]) for r in rows))
    copies_w = max(6, max(len(r["copies"]) for r in rows))
    reviews_w = max(7, max(len(r["reviews"]) for r in rows))
    score_w = max(5, max(len(r["score"]) for r in rows))
    play_w = max(8, max(len(r["play"]) for r in rows))

    header = (f"  { 'Rank':>{rank_w}}  "
              f"{'Name':<{max_name_len}}  "
              f"{'Price':>{price_w}}  "
              f"{'Revenue':>{revenue_w}}  "
              f"{'Copies':>{copies_w}}  "
              f"{'Reviews':>{reviews_w}}  "
              f"{'Score':>{score_w}}  "
              f"{'AvgPlaytime':>{play_w}}")
    lines.append(header)

    for r in rows:
        name = r["name"]
        if len(name) > max_name_len:
            name = name[:max_name_len - 3] + "..."
        line = (f"  {r['rank']:>{rank_w}}  "
                f"{name:<{max_name_len}}  "
                f"{r['price']:>{price_w}}  "
                f"{r['revenue']:>{revenue_w}}  "
                f"{r['copies']:>{copies_w}}  "
                f"{r['reviews']:>{reviews_w}}  "
                f"{r['score']:>{score_w}}  "
                f"{r['play']:>{play_w}}")
        lines.append(line)

    lines.append("")

    # --- Top tags block ---
    tag_counter: Counter = Counter()
    for r in results:
        tags = r.get("tags") or []
        if not isinstance(tags, list):
            # defensive: if tags is a string, normalize quickly
            tags = normalize_tags(tags)
        for t in tags:
            tag_counter[t] += 1

    lines.append("Top 10 tags of all games")
    if not tag_counter:
        lines.append("  (no tags found in dataset)")
        lines.append("")
        return "\n".join(lines)

    top_tags = tag_counter.most_common(10)
    # compute widths
    rank_w = max(4, len(str(len(top_tags))))
    tag_w = max(3, min(40, max(len(t[0]) for t in top_tags)))
    freq_w = max(9, max(len(str(t[1])) for t in top_tags))

    lines.append(f"  { 'Rank':>{rank_w}}  {'Tag':<{tag_w}}  {'Frequency':>{freq_w}}")
    for i, (tag, freq) in enumerate(top_tags, start=1):
        lines.append(f"  {i:>{rank_w}}  {tag:<{tag_w}}  {freq:>{freq_w}}")

    lines.append("")
    return "\n".join(lines)

# ----------------- CLI -----------------
def parse_args():
    p = argparse.ArgumentParser(description="Fetch from Gamalytic and compute stats; write results.txt")
    p.add_argument("--appids", help="file with appids (one per line)")
    p.add_argument("--api-key", help="Gamalytic API key (or set GAMALYTIC_API_KEY env var)")
    p.add_argument("--out-data", default="gamalytic.csv", help="where to save fetched data (csv or json)")
    p.add_argument("--format", choices=("csv", "json"), default="csv", help="output data format")
    p.add_argument("--out-results", default="results.txt", help="path to results text file (utf-8)")
    p.add_argument("--concurrency", "-c", type=int, default=5, help="concurrency for requests")
    p.add_argument("--timeout", type=int, default=30, help="per-request timeout (seconds)")
    p.add_argument("--delay", type=float, default=0.0, help="delay (seconds) BEFORE each request (default 0.0)")
    p.add_argument("--top", type=int, default=10, help="how many top games to list")
    p.add_argument("--add-f2p", action="store_true", help="also include games with price == 0 (F2P) in saved data and stats")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--fetch-only", action="store_true", help="only fetch & save data, do not compute stats")
    group.add_argument("--stats-only", action="store_true", help="only compute stats from existing --out-data, do not fetch")
    # new rate-limit flags
    p.add_argument("--rps", type=float, default=1.0, help="initial requests-per-second (token bucket). Default 1.0")
    p.add_argument("--rps-min", type=float, default=0.1, help="minimum rps after reductions. Default 0.1")
    p.add_argument("--rps-max", type=float, default=5.0, help="maximum rps allowed. Default 5.0")
    p.add_argument("--rps-increase", type=float, default=0.05, help="additive increase to rps on success. Default 0.05")
    p.add_argument("--rps-decrease-factor", type=float, default=0.5, help="multiplicative factor to decrease rps on 429. Default 0.5")
    return p.parse_args()


async def main_async(args):
    fetch_only = args.fetch_only
    stats_only = args.stats_only

    if stats_only:
        try:
            results = load_data(args.out_data, fmt=args.format)
        except FileNotFoundError:
            print("Data file not found for stats-only:", args.out_data)
            return 2
        if not args.add_f2p:
            results = [r for r in results if not (r.get("price") is not None and float(r.get("price")) == 0.0)]
        results_text = build_results_text(results, top_n=args.top)
        Path(args.out_results).write_text(results_text, encoding="utf-8")
        print(f"Wrote stats to {args.out_results}")
        print("\n--- Results (preview) ---\n")
        print(results_text)
        return 0

    if not args.appids:
        print("Error: --appids is required unless --stats-only is used.")
        return 2

    appids_path = Path(args.appids)
    if not appids_path.exists():
        print("appids file not found:", appids_path)
        return 2
    appids: List[str] = []
    with appids_path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            appids.append(ln.split()[0])

    api_key = args.api_key or os.environ.get("GAMALYTIC_API_KEY")
    if not api_key:
        print("Warning: no API key provided; you may be limited.", flush=True)

    # Create adaptive token bucket with CLI params
    token_bucket = AdaptiveTokenBucket(rate=args.rps, min_rate=args.rps_min, max_rate=args.rps_max,
                                       increase=args.rps_increase, decrease_factor=args.rps_decrease_factor)

    print(f"Fetching {len(appids)} appids with concurrency={args.concurrency}, delay={args.delay}s, initial rps={token_bucket.get_rate():.2f} ...")
    results = await fetch_all(appids, api_key, args.concurrency, args.timeout, delay=args.delay, token_bucket=token_bucket)

    if not args.add_f2p:
        filtered = [r for r in results if not (r.get("price") is not None and float(r.get("price")) == 0.0)]
    else:
        filtered = results

    save_data(filtered, args.out_data, args.format)
    print(f"Saved {len(filtered)} records (out of {len(results)}) to {args.out_data} (F2P included: {args.add_f2p})")

    if fetch_only:
        return 0

    results_text = build_results_text(filtered, top_n=args.top)
    out_results_path = Path(args.out_results)
    out_results_path.write_text(results_text, encoding="utf-8")
    print(f"Wrote stats to {out_results_path}")

    print("\n--- Results (preview) ---\n")
    print(results_text)
    return 0


def main():
    args = parse_args()
    try:
        rc = asyncio.run(main_async(args))
        raise SystemExit(rc or 0)
    except KeyboardInterrupt:
        print("Interrupted by user")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
