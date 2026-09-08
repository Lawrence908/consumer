#!/usr/bin/env python3
"""consumer.chrislawrence.ca data updater and read-only status API.

One narrow question: is the consumer holding up, and does what they say match
what they do? The consumer is two-thirds of the economy and confirms rather
than leads. The site's design is the say-do split itself, and this file
computes both halves of it:

  * the SLUMP table, scored: Michigan sentiment since 1952, every deep slump
    dated at its confidence peak and its slump, attributed to NBER peaks. It
    cries wolf, and the table shows exactly when;
  * the CONTRACTION table, deliberately unscored: year-over-year real personal
    consumption since 1960, three months or more in the red, attached to the
    nearest recession peak. It never cries wolf, but it only ever confirms.

Everything live on the page comes from series.json, machine-owned and
rewritten wholesale each run. data/meta.json and the vendored recessions.json
are never touched by automation. No curated figure, no hand ritual.

Guardrails, inherited from jobs: stale or shrunken upstreams are kept rather
than written, a failed fetch carries the previous series forward and records
the error, and revisions to already-published observations land in
changelog.jsonl. Retail sales arrive as an advance estimate and are revised
twice; PCE is revised monthly and benchmarked annually. The revision card is
expected busy and says so.

Three upstream facts this file knows about so nobody rediscovers them
(all probed live 2026-09-07):

  * monthly real PCE LEVELS (PCEC96) only begin in 2007. The deep monthly
    series is the index form DPCERA3M086SBEA, 1959 onward, which is what the
    contraction table computes from; index versus level changes nothing for
    year-over-year arithmetic, and the quarterly chained level reaches 1947
    for context;
  * Michigan sentiment is quarterly-cadence before 1978 and monthly after.
    The slump engine therefore averages over three ADJACENT OBSERVATIONS and
    does its lookbacks in CALENDAR MONTHS, so the pre-1978 stretch is scored
    on its own cadence rather than being resampled into a monthly series it
    never was. The rule states the basis;
  * the Canadian retail table cited in the plan (20-10-0008) is terminated,
    ending 2022-12, and its successor survey (20-10-0056) begins 2017. Both
    are fetched and both are drawn, with the 2017-2022 overlap visible. The
    redesign moved the level by about four percent; that seam is the content,
    not something to splice over.

HTTP here is read-only. Runs happen via host cron calling
`docker exec consumer-updater python /app/server.py --refresh`.
"""

import json
import math
import os
import sys
import threading
import urllib.parse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import econcore

FRED_KEY = os.environ.get("FRED_API_KEY", "").strip()
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")

SERIES_FILE = os.path.join(DATA_DIR, "series.json")
CHANGELOG = os.path.join(DATA_DIR, "changelog.jsonl")
STATE_FILE = os.path.join(DATA_DIR, "updater-state.json")
RECESSIONS_FILE = os.path.join(DATA_DIR, "recessions.json")

CURATED = ["meta", "recessions"]
SHRINK_TOLERANCE = 0.9
CHANGELOG_IN_PAYLOAD = 100

# What they SAY. Credit's blowout shell with the clocks flipped: there the
# early clock is the tightest spread (peak complacency), here it is the
# highest confidence (peak optimism), and the alarm is a collapse rather than
# a widening. Frozen after one tuning pass against the canonical record at the
# dry-run gate (see BUILD-PLAN.md); thresholds are content, not knobs.
#
# What that pass decided, recorded so nobody re-litigates it from scratch. A
# 20 percent gate missed 1969-70 (which bottomed at 20.2 percent below its
# peak, one reading past the line) and, with a tight six-month before-window,
# scored the post-9/11 collapse as a false alarm while leaving the March 2001
# recession credited to nothing at all. That is a scoring artefact, not a
# finding. Fifteen percent with a symmetric twelve-month window fixes both and
# reproduces the canonical record. The window is deliberately the LESS
# flattering choice: at eighteen months after, the 1958 slump would also
# collect the April 1960 recession and the table would show a perfect record.
# It does not deserve one. Sentiment barely moved into the 1960-61 recession
# (under six percent below its peak), so that recession stays uncredited and
# the miss is printed beside the false alarms.
SLUMP_RULE = {
    "series": "us_sentiment",
    "basis": "3-observation average versus its trailing 24-month high",
    "threshold_below_pct": 15.0,
    "sustain_observations": 2,
    "merge_gap_months": 9,
    "peak_lookback_months": 24,
    "window_before_months": 12,
    "window_after_months": 12,
    "statement": ("A slump is a stretch where the average of the last three "
                  "readings of Michigan consumer sentiment sits at least 15 "
                  "percent below its highest such average over the prior two "
                  "years, for at least two readings in a row; stretches "
                  "separated by fewer than nine clear months merge into one. "
                  "Each slump is dated two ways: the CONFIDENCE PEAK, the "
                  "most optimistic reading in the two years before the slump "
                  "began, and the SLUMP, the first reading past the "
                  "threshold. An NBER peak within twelve months either side "
                  "of the slump, measured from its start and from its last "
                  "signalling reading, is assigned to the nearest slump; lead "
                  "time is reported from both clocks. The survey ran "
                  "quarterly before 1978 and monthly after, so the average is "
                  "taken over three adjacent readings and the lookback in "
                  "calendar months; the early era is scored on its own "
                  "cadence, not resampled into a monthly series it never "
                  "was."),
}

# What they DO. Lending's lag engine verbatim, pointed at real spending. The
# point of this table is not prediction; it is the sign of the lag.
CONTRACTION_RULE = {
    "series": "us_real_pce_yoy",
    "basis": "year-over-year percent change of real personal consumption, monthly",
    "sustain_months": 3,
    "merge_gap_months": 6,
    "attach_radius_months": 24,
    "statement": ("A contraction is three or more consecutive months of "
                  "negative year-over-year real personal consumption "
                  "spending; stretches separated by fewer than six clear "
                  "months merge into one. Each is attached to the nearest "
                  "NBER peak within two years, and the lag is measured from "
                  "that peak: positive means the spending contraction began "
                  "after the recession did. This table is deliberately not a "
                  "predictor and is not scored as one. Its whole content is "
                  "how rare these rows are and what sign the lag carries."),
}

_payload_cache = {"stamp": None, "body": None}
_state = {"last_run": None, "results": []}
_lock = threading.Lock()


# --------------------------------------------------------------------------
# the series list
#
# Adding a series is a human decision with a verified source; the updater only
# refreshes what is declared. Depths in the notes were probed live on
# 2026-09-07, not assumed. StatCan vectors were resolved from cube metadata
# (20-10-0008, 20-10-0056, 36-10-0104) and carry expect_title so a renumbered
# vector fails loudly instead of returning someone else's numbers.
# --------------------------------------------------------------------------

def _fred(series_id):
    return lambda: econcore.fred_series(series_id, FRED_KEY)


def _wds(vector_id, expect_title):
    return lambda: econcore.wds_vector(vector_id, expect_title=expect_title)


def _valet(series_name):
    return lambda: econcore.valet_series(series_name)


RETAIL_OLD_TABLE = "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=2010000801"
RETAIL_NEW_TABLE = "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=2010005601"
NIEA_TABLE = "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3610010401"
CSCE_URL = "https://www.bankofcanada.ca/publications/canadian-survey-of-consumer-expectations/"

FETCHED = [
    {
        "id": "us_sentiment",
        "fetch": _fred("UMCSENT"),
        "label": "University of Michigan consumer sentiment",
        "source": "University of Michigan Surveys of Consumers, via FRED UMCSENT",
        "source_url": "https://fred.stlouisfed.org/series/UMCSENT",
        "units": "index_1966q1_100", "freq": "monthly",
        "note": "Index, 1966-Q1 = 100, from November 1952. Quarterly cadence until 1977 and monthly from January 1978; the observations are drawn as published, with no interpolation across the early gaps. The slump table computes from this series and states the cadence in its rule.",
    },
    {
        "id": "us_sentiment_expectations",
        "fetch": _fred("UMCSENT1"),
        "label": "Michigan sentiment, the historical quarterly record",
        "source": "University of Michigan Surveys of Consumers, via FRED UMCSENT1",
        "source_url": "https://fred.stlouisfed.org/series/UMCSENT1",
        "units": "index_1966q1_100", "freq": "quarterly",
        "note": "The 1952-1977 quarterly portion published as its own series. Identical values to the head of UMCSENT; carried here so the cadence change is visible in the data and not just asserted in a footnote.",
    },
    {
        "id": "us_inflation_expected_1y",
        "fetch": _fred("MICH"),
        "label": "US inflation expected, next year (Michigan)",
        "source": "University of Michigan expected inflation, via FRED MICH",
        "source_url": "https://fred.stlouisfed.org/series/MICH",
        "units": "percent", "freq": "monthly",
        "note": "Median expected price change over the next twelve months, monthly since January 1978. The other half of what consumers say, and the direct comparison to the Bank of Canada's survey lower down the page.",
    },
    {
        "id": "us_real_pce_index",
        "fetch": _fred("DPCERA3M086SBEA"),
        "label": "US real personal consumption expenditures (index)",
        "source": "BEA real PCE, chained 2017 dollars, index 2017=100, via FRED DPCERA3M086SBEA",
        "source_url": "https://fred.stlouisfed.org/series/DPCERA3M086SBEA",
        "units": "index_2017_100", "freq": "monthly",
        "note": "Monthly since January 1959, seasonally adjusted. This is the deep monthly real spending record: the monthly LEVEL series (PCEC96) only starts in 2007, and index versus level changes nothing for year-over-year arithmetic. The contraction table computes from this series.",
    },
    {
        "id": "us_real_pce_quarterly",
        "fetch": _fred("PCECC96"),
        "label": "US real personal consumption, quarterly level",
        "source": "BEA real PCE, chained 2017 dollars, SAAR, via FRED PCECC96",
        "source_url": "https://fred.stlouisfed.org/series/PCECC96",
        "units": "USD_billions_chained_2017", "freq": "quarterly",
        "note": "Quarterly since 1947-Q1, the deepest real spending record on FRED. Carried for context and for the dollar figure; the monthly index does the scoring.",
    },
    {
        "id": "us_real_retail",
        "fetch": _fred("RRSFS"),
        "label": "US real retail and food services sales",
        "source": "Census advance retail sales deflated by BEA, via FRED RRSFS",
        "source_url": "https://fred.stlouisfed.org/series/RRSFS",
        "units": "USD_millions_chained", "freq": "monthly",
        "note": "Monthly since January 1992. Goods-heavy and therefore noisier and more cyclical than total consumption, which is why it sits beside real PCE rather than replacing it. Advance estimates are revised twice.",
    },
    {
        "id": "us_retail_nominal",
        "fetch": _fred("RSAFS"),
        "label": "US retail and food services sales, nominal",
        "source": "Census advance monthly retail trade survey, via FRED RSAFS",
        "source_url": "https://fred.stlouisfed.org/series/RSAFS",
        "units": "USD_millions", "freq": "monthly",
        "note": "Monthly since January 1992, seasonally adjusted, not deflated. The headline number the news reports; the real series above is the one that answers the question.",
    },
    {
        "id": "us_real_income",
        "fetch": _fred("DSPIC96"),
        "label": "US real disposable personal income",
        "source": "BEA real disposable personal income, chained 2017 dollars, via FRED DSPIC96",
        "source_url": "https://fred.stlouisfed.org/series/DSPIC96",
        "units": "USD_billions_chained_2017", "freq": "monthly",
        "note": "Monthly since January 1959, SAAR. What the spending has to be paid out of, eventually.",
    },
    {
        "id": "us_saving_rate",
        "fetch": _fred("PSAVERT"),
        "label": "US personal saving rate",
        "source": "BEA personal saving as a percentage of disposable income, via FRED PSAVERT",
        "source_url": "https://fred.stlouisfed.org/series/PSAVERT",
        "units": "percent", "freq": "monthly",
        "note": "Monthly since January 1959. The buffer between what they earn and what they spend; it fell from double digits across the 1960s and 1970s to low single digits, and it is revised hard whenever income is.",
    },
    {
        "id": "ca_consumption",
        "fetch": _wds(62305724, "Household final consumption expenditure"),
        "label": "Canada household final consumption expenditure",
        "source": "StatCan table 36-10-0104-01, chained 2017 dollars, SAAR, vector v62305724",
        "source_url": NIEA_TABLE,
        "units": "CAD_millions_chained_2017", "freq": "quarterly",
        "note": "Quarterly since 1961-Q1, live. The deep Canadian anchor for what households actually do, on the same real basis as the US series above.",
    },
    {
        "id": "ca_retail_closed",
        "fetch": _wds(52367097, "Retail trade"),
        "label": "Canada retail trade, 1991-2022 (closed survey)",
        "source": "StatCan table 20-10-0008-01 (inactive), Canada total, seasonally adjusted, vector v52367097",
        "source_url": RETAIL_OLD_TABLE,
        "units": "CAD_thousands", "freq": "monthly",
        "note": "TERMINATED SERIES: monthly January 1991 through December 2022, when the survey was replaced. Fetched anyway; WDS serves closed history, and the page draws it ending where it ended.",
    },
    {
        "id": "ca_retail_current",
        "fetch": _wds(1446859483, "Total retail sales"),
        "label": "Canada retail trade, 2017 onward (successor survey)",
        "source": "StatCan table 20-10-0056-01, Canada total retail sales, seasonally adjusted, vector v1446859483",
        "source_url": RETAIL_NEW_TABLE,
        "units": "CAD_thousands", "freq": "monthly",
        "note": "The replacement survey, monthly from January 2017 and live. It overlaps the closed series for six years and reads about four percent higher on the same months. That overlap is drawn rather than spliced away: the gap is the survey redesign, and pretending otherwise would invent a jump that never happened.",
    },
    {
        "id": "ca_expect_inflation_1y",
        "fetch": _valet("CES_C1_SHORT_TERM"),
        "label": "Canada inflation expected, next year",
        "source": "Bank of Canada Canadian Survey of Consumer Expectations, Valet series CES_C1_SHORT_TERM",
        "source_url": CSCE_URL,
        "units": "percent", "freq": "quarterly",
        "note": "Mean expected inflation one year ahead, quarterly since 2014-Q4. No free Canadian consumer sentiment index exists (the Conference Board's is paywalled), so this survey stands in for the Canadian say-side, and the page says so rather than pretending the gap is not there.",
    },
    {
        "id": "ca_perceived_inflation",
        "fetch": _valet("CES_C1_PERCEPTIONS"),
        "label": "Canada inflation perceived, past year",
        "source": "Bank of Canada CSCE, Valet series CES_C1_PERCEPTIONS",
        "source_url": CSCE_URL,
        "units": "percent", "freq": "quarterly",
        "note": "What consumers believe inflation has already been over the past twelve months, quarterly since 2014-Q4. Perceptions and expectations move together and both sit above measured CPI.",
    },
    {
        "id": "ca_expect_inflation_5y",
        "fetch": _valet("CES_C1_LONG_TERM"),
        "label": "Canada inflation expected, five years ahead",
        "source": "Bank of Canada CSCE, Valet series CES_C1_LONG_TERM",
        "source_url": CSCE_URL,
        "units": "percent", "freq": "quarterly",
        "note": "The long-horizon expectation, quarterly since 2015-Q2. The anchor the Bank watches hardest.",
    },
]


# --------------------------------------------------------------------------
# derived series: the constructions, stated
# --------------------------------------------------------------------------

DERIVED = [
    ("us_real_pce_index", "us_real_pce_yoy", 12, "monthly",
     "US real consumption growth, year over year",
     "The doing side's scored series. Computed here from the real PCE index in this payload; the index and the level give identical year-over-year arithmetic."),
    ("us_real_retail", "us_real_retail_yoy", 12, "monthly",
     "US real retail sales growth, year over year",
     "Computed here from the deflated retail series in this payload."),
    ("us_retail_nominal", "us_retail_nominal_yoy", 12, "monthly",
     "US nominal retail sales growth, year over year",
     "Computed here. Shown against the real series so the inflation wedge between the headline and the substance is visible."),
    ("us_real_income", "us_real_income_yoy", 12, "monthly",
     "US real disposable income growth, year over year",
     "Computed here from the real disposable income series in this payload."),
    ("ca_consumption", "ca_consumption_yoy", 4, "quarterly",
     "Canada household consumption growth, year over year",
     "Computed here from the chained-dollar quarterly level in this payload, four quarters apart."),
]


def build_derived(series):
    """Year-over-year growth, computed here with the construction stated;
    every input ships as a reported series in the same payload."""
    out = {}
    for src_id, new_id, periods, freq, label, note in DERIVED:
        src = series.get(src_id)
        if not src or len(src["obs"]) <= periods:
            continue
        out[new_id] = econcore.make_series(
            new_id, label,
            "Derived: %d-period percent change of %s" % (periods, src["source"]),
            src["source_url"], "percent", freq,
            [[d, round(v, 2)] for d, v in
             econcore.yoy_percent(src["obs"], periods)],
            confidence="estimate", note=note)
    return out


# --------------------------------------------------------------------------
# analysis: status, the slump table, the contraction table
# --------------------------------------------------------------------------

def _mi(year_month):
    year, month = year_month.split("-")[:2]
    return int(year) * 12 + int(month) - 1


def _three_obs_avg(obs):
    """Average of three ADJACENT observations, not of three calendar months.

    Michigan sentiment is quarterly before 1978 and monthly after. Averaging
    by adjacency means the early era is smoothed over three quarters and the
    modern era over three months, each on its own cadence; averaging by
    calendar month would have required inventing readings that were never
    taken. The rule says which basis is in force.
    """
    return [[obs[i][0], (obs[i][1] + obs[i - 1][1] + obs[i - 2][1]) / 3.0]
            for i in range(2, len(obs))]


def _runs_and_groups(flags_idx, months, merge_gap):
    """Consecutive qualifying observations into runs, runs into merged groups.

    Runs use observation adjacency; the merge gap is calendar months. Shared
    verbatim with lending, where both tables use the same shape.
    """
    runs = [[flags_idx[0], flags_idx[0]]]
    for i in flags_idx[1:]:
        if i == runs[-1][1] + 1:
            runs[-1][1] = i
        else:
            runs.append([i, i])
    groups = [runs[0][:]]
    for start_i, end_i in runs[1:]:
        gap = _mi(months[start_i][0]) - _mi(months[groups[-1][1]][0]) - 1
        if gap < merge_gap:
            groups[-1][1] = end_i
        else:
            groups.append([start_i, end_i])
    return groups


def _median(values):
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    return (ordered[mid] if len(ordered) % 2
            else (ordered[mid - 1] + ordered[mid]) / 2.0)


# The chip's own rule: SLUMP_RULE's threshold without the sustain and merge
# conditions, which date completed episodes rather than describe today.
CHIP_RULE = ("The slump signal is on when the average of the last three "
             "sentiment readings sits 15 percent or more below its best such "
             "average over the prior two years. The label names the say-do "
             "gap: which of the two halves, what they say and what they do, "
             "is currently weak.")

SAY_DO_LABELS = {
    "gap_open": "Sentiment slumped, spending growing",
    "both_weak": "Sentiment slumped, spending contracting",
    "spending_only": "Sentiment steady, spending contracting",
    "both_steady": "Sentiment steady, spending growing",
}


def _signed(value, places=1):
    """The page's sign convention: U+2212 for negatives, never a hyphen."""
    sign = "+" if value > 0 else ("−" if value < 0 else "")
    return "%s%.*f%%" % (sign, places, abs(value))


def build_status(series):
    """The launch posture, computed. The say-do gap is two booleans and the
    numbers behind them; the chip renders from these tokens."""
    status = {}

    sent = series.get("us_sentiment")
    if sent and len(sent["obs"]) > 3:
        obs = sent["obs"]
        latest = obs[-1]
        values = [v for _, v in obs]
        at_or_below = len([v for v in values if v <= latest[1]])
        low = min(obs, key=lambda o: o[1])
        high = max(obs, key=lambda o: o[1])
        avgs = _three_obs_avg(obs)
        cur_avg = avgs[-1]
        look_lo = _mi(cur_avg[0][:7]) - SLUMP_RULE["peak_lookback_months"]
        prior = [a for a in avgs if look_lo <= _mi(a[0][:7]) < _mi(cur_avg[0][:7])]
        peak = max(prior, key=lambda a: a[1]) if prior else None
        below = ((peak[1] - cur_avg[1]) / peak[1] * 100.0) if peak else None
        status["us_sentiment"] = {
            "latest": [latest[0], latest[1]],
            "observations": len(obs),
            "percentile": round(at_or_below / float(len(obs)) * 100.0, 1),
            "record_low": [low[0], low[1]],
            "record_high": [high[0], high[1]],
            "first": obs[0][0],
            "latest_3avg": [cur_avg[0], round(cur_avg[1], 1)],
            "peak_3avg_24m": ([peak[0], round(peak[1], 1)] if peak else None),
            "below_peak_pct": (round(below, 1) if below is not None else None),
        }
        status["slump_active"] = (below is not None
                                  and below >= SLUMP_RULE["threshold_below_pct"])

    pce = series.get("us_real_pce_yoy")
    if pce and pce["obs"]:
        status["us_real_pce_yoy"] = {"latest": [pce["obs"][-1][0],
                                                pce["obs"][-1][1]]}
        status["spending_contracting"] = pce["obs"][-1][1] < 0

    for sid in ("us_real_retail_yoy", "us_real_income_yoy",
                "us_saving_rate", "us_inflation_expected_1y",
                "ca_consumption_yoy", "ca_expect_inflation_1y"):
        entry = series.get(sid)
        if entry and entry["obs"]:
            status[sid] = {"latest": [entry["obs"][-1][0], entry["obs"][-1][1]]}

    saving = series.get("us_saving_rate")
    if saving and len(saving["obs"]) > 12:
        values = [v for _, v in saving["obs"]]
        latest_v = values[-1]
        status["us_saving_rate"]["percentile"] = round(
            len([v for v in values if v <= latest_v]) / float(len(values)) * 100.0, 1)

    # The say-do gap itself, as one computed token rather than a sentence
    # anybody has to remember to update.
    if "slump_active" in status and "spending_contracting" in status:
        if status["slump_active"] and status["spending_contracting"]:
            status["say_do"] = "both_weak"
        elif status["slump_active"]:
            status["say_do"] = "gap_open"
        elif status["spending_contracting"]:
            status["say_do"] = "spending_only"
        else:
            status["say_do"] = "both_steady"

    # The status block the hub reads. This page's state is the say-do gap
    # itself rather than either series alone, so the label names the gap and
    # signal means the two halves disagree: a mood that has broken while the
    # spending has not is exactly the condition worth surfacing.
    sent_status = status.get("us_sentiment")
    if sent_status and "say_do" in status:
        signal = status["say_do"] in ("gap_open", "both_weak")
        detail = "sentiment %.1f (bottom %d%% since 1952)" % (
            sent_status["latest"][1], math.ceil(sent_status["percentile"]))
        pce = status.get("us_real_pce_yoy")
        if pce:
            detail += " · real spending %s year over year" % _signed(
                pce["latest"][1])
        status["signal_active"] = signal
        status["headline"] = {
            "state": "signal" if signal else "normal",
            "label": SAY_DO_LABELS.get(status["say_do"], "Consumer"),
            "detail": detail,
            "as_of": sent_status["latest"][0],
            "rule": CHIP_RULE,
        }
    return status


def build_slumps(sent_entry, recessions):
    """The slump table: credit's blowout shell with both clocks flipped.

    There the early clock is the tightest spread before the widening; here it
    is the most optimistic reading before the collapse. There the alarm is a
    spread widening past a threshold; here it is sentiment falling far enough
    below its own recent best. Lookbacks are done in calendar months against
    an observation-adjacent average, because the survey changes cadence in
    1978 and neither half should be resampled into the other.
    """
    avgs = _three_obs_avg(sent_entry["obs"])
    months = [[d[:7], v] for d, v in avgs]
    look = SLUMP_RULE["peak_lookback_months"]
    threshold = SLUMP_RULE["threshold_below_pct"]

    # Depth below the trailing high, by calendar month rather than by index:
    # a 24-observation window would reach back six years in the quarterly era.
    below = []
    for i, (month, value) in enumerate(months):
        lo_mi = _mi(month) - look
        prior = [v for m, v in months[:i] if _mi(m) >= lo_mi]
        if not prior:
            continue
        high = max(prior)
        if high <= 0:
            continue
        below.append([month, (high - value) / high * 100.0])

    qualifying = [i for i, (_, d) in enumerate(below) if d >= threshold]
    if not qualifying:
        return {"rule": SLUMP_RULE, "episodes": [], "stats": {}}

    groups = _runs_and_groups(qualifying, below, SLUMP_RULE["merge_gap_months"])

    def longest_run(lo, hi):
        best = run = 0
        for i in range(lo, hi + 1):
            run = run + 1 if below[i][1] >= threshold else 0
            best = max(best, run)
        return best

    groups = [g for g in groups
              if longest_run(g[0], g[1]) >= SLUMP_RULE["sustain_observations"]]

    shells = []
    for lo, hi in groups:
        span = below[lo:hi + 1]
        start, end = span[0][0], span[-1][0]
        lo_mi = _mi(start) - look
        prior = [m for m in months if lo_mi <= _mi(m[0]) < _mi(start)]
        peak = max(prior, key=lambda m: m[1]) if prior else None
        # The bottom of the slump itself, searched from the alarm through the
        # last signalling reading plus two quarters: sentiment often keeps
        # falling after it has already qualified.
        within = [m for m in months
                  if _mi(start) <= _mi(m[0]) <= _mi(end) + 6]
        trough = min(within, key=lambda m: m[1])
        shells.append({
            "start": start, "end": end, "span": span,
            "peak": peak, "trough": trough,
            "left_censored": peak is None,
            "window_lo": _mi(start) - SLUMP_RULE["window_before_months"],
            "window_hi": _mi(end) + SLUMP_RULE["window_after_months"],
            "peaks": [],
        })

    bands = recessions["us"]["bands"]
    data_through = _mi(recessions["us"]["as_of"][:7])
    first_mi = _mi(months[0][0])
    assigned = set()
    for band in bands:
        peak_month = band["peak"]
        candidates = [s for s in shells
                      if s["window_lo"] <= _mi(peak_month) <= s["window_hi"]]
        if not candidates:
            continue
        best = max(candidates, key=lambda s: _mi(s["start"]))
        best["peaks"].append(peak_month)
        assigned.add(peak_month)

    episodes = []
    for s in shells:
        led = [p for p in s["peaks"] if _mi(p) >= _mi(s["start"])]
        if led:
            outcome = "recession"
        elif s["peaks"]:
            outcome = "coincident"
        elif s["window_hi"] > data_through:
            outcome = "pending"
        else:
            outcome = "none_in_window"
        first_peak = s["peaks"][0] if s["peaks"] else None
        depth = max(d for _, d in s["span"])
        episodes.append({
            "start": s["start"],
            "end": s["end"],
            "left_censored": s["left_censored"],
            "confidence_peak": ({"month": s["peak"][0],
                                 "value": round(s["peak"][1], 1)}
                                if s["peak"] else None),
            "trough": {"month": s["trough"][0],
                       "value": round(s["trough"][1], 1)},
            "deepest_below_pct": round(depth, 1),
            "observations_signalling": len([1 for _, d in s["span"]
                                            if d >= threshold]),
            "months_span": _mi(s["end"]) - _mi(s["start"]) + 1,
            "recessions": s["peaks"],
            "lead_from_peak_months": (_mi(first_peak) - _mi(s["peak"][0])
                                      if first_peak and s["peak"] else None),
            "lead_from_slump_months": (_mi(first_peak) - _mi(s["start"])
                                       if first_peak else None),
            "outcome": outcome,
        })

    # An episode is still running when its last signalling reading is the last
    # reading the survey has published.
    last_month = months[-1][0]
    for e in episodes:
        e["still_running"] = e["end"] == last_month

    leads = [e["lead_from_slump_months"] for e in episodes if e["recessions"]]
    stats = {}
    if leads:
        stats = {
            "episodes": len(episodes),
            "credited_episodes": len(leads),
            "median_lead_from_slump_months": _median(leads),
            "median_lead_from_peak_months": _median(
                [e["lead_from_peak_months"] for e in episodes
                 if e["lead_from_peak_months"] is not None]),
            "slump_led": len([e for e in episodes
                              if e["outcome"] == "recession"]),
            "coincident": len([e for e in episodes
                               if e["outcome"] == "coincident"]),
            "false_positives": len([e for e in episodes
                                    if e["outcome"] == "none_in_window"]),
            "pending": len([e for e in episodes if e["outcome"] == "pending"]),
            "false_positive_years": [e["start"][:4] for e in episodes
                                     if e["outcome"] == "none_in_window"],
            "uncredited_recessions": [b["peak"] for b in bands
                                      if _mi(b["peak"]) >= first_mi
                                      and b["peak"] not in assigned],
        }
    return {"rule": SLUMP_RULE, "episodes": episodes, "stats": stats}


def build_contractions(yoy_entry, recessions):
    """The lag table: real spending contractions attached to the nearest
    recession peak. Lending's engine verbatim, pointed at consumption. Not a
    predictor and not scored as one; the sign of the lag is the finding, and
    so is how few rows there are."""
    months = [[d[:7], v] for d, v in yoy_entry["obs"]]
    qualifying = [i for i, (_, v) in enumerate(months) if v < 0]
    if not qualifying:
        return {"rule": CONTRACTION_RULE, "contractions": [], "stats": {}}

    groups = _runs_and_groups(qualifying, months,
                              CONTRACTION_RULE["merge_gap_months"])

    def longest_run(lo, hi):
        best = run = 0
        for i in range(lo, hi + 1):
            run = run + 1 if months[i][1] < 0 else 0
            best = max(best, run)
        return best

    groups = [g for g in groups
              if longest_run(g[0], g[1]) >= CONTRACTION_RULE["sustain_months"]]

    bands = recessions["us"]["bands"]
    radius = CONTRACTION_RULE["attach_radius_months"]
    contractions = []
    for lo, hi in groups:
        span = months[lo:hi + 1]
        start, end = span[0][0], span[-1][0]
        deepest = min(span, key=lambda m: m[1])
        nearest_peak, nearest_gap = None, None
        for band in bands:
            gap = _mi(start) - _mi(band["peak"])
            if abs(gap) <= radius and (nearest_gap is None
                                       or abs(gap) < abs(nearest_gap)):
                nearest_peak, nearest_gap = band["peak"], gap
        contractions.append({
            "start": start,
            "end": end,
            "months": _mi(end) - _mi(start) + 1,
            "deepest": {"month": deepest[0], "value": round(deepest[1], 1)},
            "recession_peak": nearest_peak,
            "lag_months": nearest_gap,
        })

    lags = [c["lag_months"] for c in contractions if c["lag_months"] is not None]
    stats = {}
    if contractions:
        stats = {"total": len(contractions),
                 "attached": len(lags),
                 "unattached": len(contractions) - len(lags),
                 "median_lag_months": _median(lags),
                 "began_after_recession": len([l for l in lags if l > 0]),
                 "first_month": months[0][0],
                 "months_covered": _mi(months[-1][0]) - _mi(months[0][0]) + 1,
                 "months_in_contraction": sum(c["months"] for c in contractions)}
    return {"rule": CONTRACTION_RULE, "contractions": contractions,
            "stats": stats}


def build_analysis(series):
    analysis = {"status": build_status(series)}
    try:
        recessions = econcore.load_recessions(RECESSIONS_FILE)
    except Exception as exc:  # noqa: BLE001 - both tables degrade, page renders
        analysis["slumps_error"] = "%s: %s" % (type(exc).__name__, exc)
        return analysis
    sent = series.get("us_sentiment")
    if sent:
        try:
            analysis["slumps"] = build_slumps(sent, recessions)
        except Exception as exc:  # noqa: BLE001
            analysis["slumps_error"] = "%s: %s" % (type(exc).__name__, exc)
    pce = series.get("us_real_pce_yoy")
    if pce:
        try:
            analysis["contractions"] = build_contractions(pce, recessions)
        except Exception as exc:  # noqa: BLE001
            analysis["contractions_error"] = "%s: %s" % (type(exc).__name__, exc)
    return analysis


# --------------------------------------------------------------------------
# refresh
# --------------------------------------------------------------------------

def load_old_series():
    try:
        with open(SERIES_FILE) as fh:
            return json.load(fh).get("series", {})
    except Exception:  # noqa: BLE001 - first run, or corrupt file: start clean
        return {}


def _diff_revisions(series_id, old_obs, new_obs):
    """Changed values at already-published dates, raw fetched series only."""
    old_map = dict(map(tuple, old_obs))
    changed = [(d, old_map[d], v) for d, v in new_obs
               if d in old_map and abs(old_map[d] - v) > 1e-9]
    if not changed:
        return None
    deltas = [abs(after - before) for _, before, after in changed]
    return {
        "series": series_id, "action": "revised",
        "changed": len(changed),
        "span": [changed[0][0], changed[-1][0]],
        "max_delta": round(max(deltas), 4),
        "sample": [{"date": d, "before": b, "after": a}
                   for d, b, a in changed[:3]],
    }


def refresh_series(dry=False):
    old = load_old_series()
    series, errors, results = {}, {}, []

    for spec in FETCHED:
        sid = spec["id"]
        prev = old.get(sid)
        rec = {"series": sid, "action": "fetched"}
        try:
            obs = spec["fetch"]()
            doc = econcore.make_series(
                sid, spec["label"], spec["source"], spec["source_url"],
                spec["units"], spec["freq"], obs, note=spec.get("note"))
            if prev and prev.get("obs"):
                if doc["as_of"] < prev["as_of"]:
                    rec.update(action="stale-upstream",
                               reason="upstream at %s, behind stored %s; kept"
                                      % (doc["as_of"], prev["as_of"]))
                    doc = prev
                elif len(obs) < len(prev["obs"]) * SHRINK_TOLERANCE:
                    rec.update(action="shrunk",
                               reason="%d obs against %d stored; kept"
                                      % (len(obs), len(prev["obs"])))
                    doc = prev
                else:
                    revision = _diff_revisions(sid, prev["obs"], obs)
                    if revision and prev.get("source") == doc.get("source"):
                        if not dry:
                            econcore.log_revision(CHANGELOG, revision)
                        rec.update(action="revised",
                                   changed=revision["changed"])
                    added = len(obs) - len(prev["obs"])
                    if added > 0:
                        rec["added"] = added
            series[sid] = doc
        except Exception as exc:  # noqa: BLE001 - one dead endpoint, one chart
            errors[sid] = "%s: %s" % (type(exc).__name__, exc)
            rec.update(action="error", reason=errors[sid])
            if prev:
                series[sid] = prev
                rec["carried_forward"] = True
        results.append(rec)
        print("%-26s %-14s %s" % (sid, rec["action"], rec.get("reason", "")),
              flush=True)

    if not series:
        raise ValueError("nothing fetched and nothing stored; refusing to write")

    series.update(build_derived(series))
    analysis = build_analysis(series)

    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "note": "Machine-fetched. Never hand-edited; the updater rewrites this file wholesale.",
        "econcore": econcore.VERSION,
        "fred_key_used": bool(FRED_KEY),
        "errors": errors,
        "series": series,
        "analysis": analysis,
    }

    if dry:
        total = sum(len(s["obs"]) for s in series.values())
        print("dry run: %d series, %d observations, %d errors -- not written"
              % (len(series), total, len(errors)), flush=True)
        return payload

    tmp = SERIES_FILE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    os.chmod(tmp, 0o644)
    os.replace(tmp, SERIES_FILE)

    with _lock:
        _state["last_run"] = datetime.now(timezone.utc).isoformat()
        _state["results"] = results
    _save_state()

    total = sum(len(s["obs"]) for s in series.values())
    print("series refreshed: %d series, %d observations, %d errors"
          % (len(series), total, len(errors)), flush=True)
    return payload


def _save_state():
    try:
        with _lock:
            snapshot = dict(_state)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(snapshot, fh, indent=2)
        os.chmod(tmp, 0o644)
        os.replace(tmp, STATE_FILE)
    except OSError:
        pass


# --------------------------------------------------------------------------
# read-only HTTP
# --------------------------------------------------------------------------

def _load(name):
    with open(os.path.join(DATA_DIR, name)) as fh:
        return json.load(fh)


def data_stamp():
    newest = 0.0
    names = [n + ".json" for n in CURATED] + ["series.json", "changelog.jsonl"]
    for name in names:
        try:
            newest = max(newest, os.path.getmtime(os.path.join(DATA_DIR, name)))
        except OSError:
            continue
    return newest


def build_data_payload():
    stamp = data_stamp()
    if _payload_cache["stamp"] == stamp and _payload_cache["body"] is not None:
        return _payload_cache["body"]

    payload = {"generated_at": datetime.now(timezone.utc).isoformat()}
    for name in CURATED:
        try:
            payload[name] = _load(name + ".json")
        except Exception as exc:  # noqa: BLE001 - reported, not fatal
            payload[name] = None
            payload.setdefault("errors", {})[name] = str(exc)
    try:
        doc = _load("series.json")
        payload["series"] = doc.get("series", {})
        # The stored block is written by the refresh, which runs out of
        # process; one written before the status contract existed has no
        # headline, and the chip would stay hidden until the next scheduled
        # run. Recomputing the cheap half here makes a deploy take effect now.
        analysis = dict(doc.get("analysis", {}))
        if "headline" not in (analysis.get("status") or {}):
            analysis["status"] = build_status(payload["series"])
        payload["analysis"] = analysis
        payload["series_fetched_at"] = doc.get("fetched_at")
        payload["series_errors"] = doc.get("errors", {})
    except Exception as exc:  # noqa: BLE001 - charts degrade, page renders
        payload["series"] = {}
        payload["analysis"] = {}
        payload.setdefault("errors", {})["series"] = str(exc)

    recent, total = econcore.read_revisions(CHANGELOG, CHANGELOG_IN_PAYLOAD)
    payload["changelog"] = {"total": total, "recent": recent}

    _payload_cache["stamp"] = stamp
    _payload_cache["body"] = payload
    return payload


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code, body, cache="no-cache"):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", cache)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        path = urllib.parse.urlparse(self.path).path
        if path == "/api/health":
            # Probe the dependency, not the process: no data, not healthy.
            try:
                doc = _load("series.json")
                sent = doc.get("series", {}).get("us_sentiment", {})
                st = doc.get("analysis", {}).get("status", {})
                self._send(200, {
                    "status": "ok",
                    "series": len(doc.get("series", {})),
                    "latest": sent.get("as_of"),
                    "headline": st.get("headline"),
                    "signal_active": st.get("signal_active"),
                    "say_do": st.get("say_do"),
                    "slump_active": st.get("slump_active"),
                    "errors": len(doc.get("errors", {})),
                    "fetched_at": doc.get("fetched_at"),
                })
            except Exception as exc:  # noqa: BLE001 - absent data IS the unhealthy case
                self._send(503, {"status": "no data", "error": str(exc)})
        elif path == "/api/data":
            self._send(200, build_data_payload(),
                       cache="public, max-age=300, must-revalidate")
        elif path == "/api/status":
            with _lock:
                snapshot = dict(_state)
            snapshot["fred_key"] = bool(FRED_KEY)
            snapshot["econcore"] = econcore.VERSION
            self._send(200, snapshot)
        elif path == "/api/changelog":
            recent, total = econcore.read_revisions(CHANGELOG, CHANGELOG_IN_PAYLOAD)
            self._send(200, {"total": total, "recent": recent})
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        return


def main():
    if "--refresh" in sys.argv:
        refresh_series()
        return
    if "--once" in sys.argv:
        refresh_series(dry=True)
        return

    print("updater starting: fred_key=%s (schedule: host cron)"
          % bool(FRED_KEY), flush=True)

    def warm():
        try:
            refresh_series()
        except Exception as exc:  # noqa: BLE001 - server must come up regardless
            print("initial fetch failed: %s" % exc, flush=True)

    threading.Thread(target=warm, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()


if __name__ == "__main__":
    main()
