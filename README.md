# Is the Consumer Holding Up?

What they say against what they do. Sentiment slumps often and sometimes wrongly; real
spending has contracted year over year five times since 1960, always after the recession
had already begun. This page computes both tables and prints both rules. Live at
[consumer.chrislawrence.ca](https://consumer.chrislawrence.ca).

No framework, no build step, no package manager. Plain HTML, CSS and vanilla JS on an
nginx front, with a stdlib-Python updater sidecar. Part of the economic tracker
collection (diesel, debt, jobs, yield, housing, credit, lending, freight) on the shared
[`econ-core`](https://github.com/Lawrence908/econ-core/blob/main/CONTRACT.md) series contract.

## Layout

```
src/index.html    markup, styling, the TimeChart canvas engine, every render function
data/series.json  machine-fetched, rewritten wholesale each run, never hand-edited
data/meta.json    curated; deliberately near-empty (no hand-entered figure exists here)
data/recessions.json  vendored from econ-core; never edited here
api/server.py     updater, both computed engines, and read-only status API
api/econcore.py   vendored, stamped copy of the shared fetchers
```

## The series

Twenty series on the econ-core contract. **The saying side**: Michigan consumer sentiment
since November 1952 (quarterly until 1977, monthly after, with the historical quarterly
portion shipped separately so the cadence change is visible in the data), and Michigan
expected inflation since 1978. **The doing side**: real personal consumption as the index
form since January 1959, the quarterly chained level back to 1947-Q1, real and nominal
retail sales since 1992, real disposable income and the saving rate since 1959.
**Canada**: household final consumption in chained 2017 dollars quarterly since 1961-Q1
(title-verified vector v62305724), both halves of the retail survey, and the Bank of
Canada's consumer expectations survey since 2014-Q4. Year-over-year variants are computed
here; nothing is deflated on this site, because every real series arrives real from its
publisher.

Two upstream facts the code knows about so nobody rediscovers them. Monthly real PCE
*levels* only start in 2007, so the deep monthly record is the index form
(`DPCERA3M086SBEA`, 1959 onward); index and level give identical year-over-year
arithmetic. And there is no free Canadian consumer confidence index at all, the
Conference Board's being paywalled, so the Canadian say-side is genuinely thinner than
the American one and the page says so rather than substituting something and implying it
is equivalent.

## Two computed tables

**The slump table, scored.** Stretches where the average of the last three sentiment
readings sits at least 15 percent below its best such average in the prior two years,
sustained two readings, merged across gaps under nine months. Each slump is dated at its
CONFIDENCE PEAK and at the slump itself. On current data: 11 slumps in 74 years. The
confidence peak leads the recessions it caught by a median 13.5 months while the slump
itself is roughly coincident (median −1). Sentiment cried wolf twice with nothing inside
the window (2005 and 2011) and slept through the 1960-61 recession entirely, when the
mood barely moved. One episode is running at the last reading and scores pending.

**The contraction table, deliberately unscored.** Three or more consecutive months of
negative year-over-year real consumption, attached to the nearest NBER peak within two
years. The finding, computed: five episodes in 67 years, 61 months out of 799, and all
five began *after* the recession had already started, median lag +2 months. The consumer
breaks last. The contrast between eleven mood collapses and five spending contractions is
the whole site.

### Where the rules came from

The slump rule took one tuning pass at the dry-run gate and was then frozen. A 20 percent
gate missed 1969-70 (which bottomed 20.2 percent below its peak, one reading past the
line) and, with a tight six-month before-window, scored the post-9/11 collapse as a false
alarm while leaving the March 2001 recession credited to nothing at all, which is a
scoring artefact rather than a finding. Fifteen percent with a symmetric twelve-month
window fixes both. That window is deliberately the less flattering choice: at eighteen
months after, the 1958 slump would also collect the April 1960 recession and the table
would show a perfect record it does not deserve. The contraction rule is lending's lag
engine verbatim and needed no tuning. Both ship in the payload and are printed beside
their tables.

## The updater

```bash
docker exec consumer-updater python /app/server.py --once      # dry run
docker exec consumer-updater python /app/server.py --refresh   # what cron runs
```

Host crontab, daily at 07:25 Pacific, log bounded monthly. Family guardrails: stale or
shrunken upstreams kept, failures carry forward with the error recorded, revisions logged
to `changelog.jsonl`. Expect that log to be busy: retail sales are published as an advance
estimate and revised twice, and income and consumption are revised monthly and benchmarked
annually. Fetch policy is econ-core's: keyless first (FRED CSV, StatCan WDS, Valet), keyed
FRED as fallback (`FRED_API_KEY` in `.env`, gitignored).

## Provenance

Assembled with Claude, made by Anthropic. Published surveys and published aggregates, with
computed history and both rules printed. No forecasts.
