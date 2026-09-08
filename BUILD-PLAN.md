# consumer.chrislawrence.ca — build plan

One narrow question: **is the consumer holding up, and does what they say match what
they do?** The consumer is two-thirds of the US economy and it confirms rather than
leads. The site's design is the split itself: sentiment (what they say) slumps often
and sometimes wrongly; real spending (what they do) has contracted year-over-year only
a handful of times in seven decades, always inside a recession. Two computed tables,
one for each mouth of that gap. This closes the family's loop.

Site nine of the family, seventh econ-core consumer. Written 2026-09-07; every series
probed live that day through econcore's fetchers.

## Verified sources

### United States (FRED, keyless, all probed)

| Series | What | Depth | Freq | Latest |
|---|---|---|---|---|
| `UMCSENT` | U Michigan consumer sentiment | **1952-11 →** | monthly* | **55.2** |
| `RSAFS` | Advance retail sales, nominal SA | 1992-01 → | monthly | $763.6B |
| `RRSFS` | Real retail and food services sales | 1992-01 → | monthly | $229.4B |
| `DPCERA3M086SBEA` | Real PCE index | **1959-01 →** | monthly | 127.2 |
| `PCECC96` | Real PCE, chained, quarterly | **1947-Q1 →** | quarterly | $16.8T |
| `DSPIC96` | Real disposable personal income | 1959-01 → | monthly | $18.1T |
| `PSAVERT` | Personal saving rate | 1959-01 → | monthly | **3.0%** |

*UMCSENT is quarterly-cadence before 1978 and monthly after; the engine works on
observation adjacency and month arithmetic, and the rule states the basis.

### Canada (StatCan WDS + BoC Valet, probed)

| Series | What | Depth | Status |
|---|---|---|---|
| `v62305724` | Household final consumption, chained 2017$, SAAR (36-10-0104) | **1961-Q1 → live** | resolved, title-verified |
| 20-10-0008 retail | Retail trade, Canada total, SA | 1991-01 → 2022-12 | **cube inactive, terminated** |
| 20-10-0056 retail | Monthly retail trade, successor survey | 2017-01 → live | the replacement |
| `CES_C1_SHORT_TERM` / `CES_C1_PERCEPTIONS` | CSCE 1y inflation expectations / perceptions | 2014-Q4 → live | quarterly, Valet |

Findings from the probe:

1. **Monthly real PCE levels (`PCEC96`) only start 2007**; the deep monthly real
   spending series is the index form `DPCERA3M086SBEA` (1959 →), which is what the
   contraction table computes from. The quarterly chained series reaches 1947 for
   context. Index versus level changes nothing for year-over-year arithmetic.
2. **The cited Canadian retail table (20-10-0008) is terminated** (inactive, ends
   2022-12); its successor survey (20-10-0056) begins 2017. The Canada card draws both
   with the 2017-2022 overlap visible, credit-site style: the overlap shows the survey
   redesign instead of hiding it, and nothing is spliced. Coordinates resolve at build
   with the usual live-title verification (old: Canada / Retail trade / SA; new:
   Canada / Retail trade / Total retail sales / SA).
3. No free Canadian sentiment index exists (Conference Board of Canada is paywalled).
   The closest keyless consumer survey is the Bank of Canada's CSCE; its 1-year
   inflation expectations and perceptions series ship as a small card with that gap
   stated. Notably still above 4% at the last reading.
4. **Launch posture is the site's thesis in one line**: sentiment at 55.2 sits in the
   bottom decile of 75 years (the record low is 50.0, June 2022) while real spending
   and real retail keep growing and the saving rate sits at just 3.0%. The say-do gap
   is wide open at launch, and the chip renders it from computed tokens.

## The features: two tables, one for each mouth

**Sentiment slumps, scored** (UMCSENT, 1952 →). The family's two clocks:

- The CONFIDENCE PEAK: the highest 3-month average in the two years before the slump.
- The SLUMP: the 3-month average at least 20% below its trailing 24-month high, at
  least two consecutive observations; episodes merging within nine clear months; NBER
  peaks assigned to the nearest episode inside [slump - 6 months, last signal + 18
  months]; the usual outcome vocabulary. Threshold tuned once at the dry-run gate
  against the canonical record, then frozen and printed.
- Expected canonical reading: slumps into every recession from 1969-70 through 2020,
  with the famous false positives intact: 2011 (the debt-ceiling summer) and the
  2021-onward mega-slump containing the all-time record low with no recession
  attached. Depending on how the 2024 partial recovery interacts with the merge rule,
  the current episode may render as one five-year slump that is still active at
  launch; whatever computes is what ships.

**Real spending contractions, deliberately unscored** (real PCE index, 1959 →).
Lending's lag-table engine verbatim: three or more consecutive months of negative
year-over-year real PCE, attached to the nearest NBER peak within two years, lag
measured from that peak. Expected: roughly four episodes (1974, 1980, 2008-09, 2020),
every one inside or after its recession. If the median lag computes positive, "the
consumer breaks last" is a computed sentence, and the contrast with the sentiment
table above it is the entire site.

## Architecture

Clone freight/lending wholesale. nginx front `consumer` (host port **8135**, verified
free) + stdlib sidecar `consumer-updater`. Vendor econ-core; jobs guardrails; the
sentiment engine is credit's trough-flipped shell (peak lookback instead of trough)
and the contraction engine is lending's; host cron daily 07:25 PT (retail mid-month,
PCE end-of-month, sentiment twice monthly, StatCan and Valet quarterly); monthly log
truncation. `data/meta.json` near-empty; no curated figure, no hand ritual. Retail
sales and PCE revise heavily (advance estimates, annual benchmarks), so the revision
card is expected busy and says so.

## Page

1. Header, the question, chip: the say-do gap live ("Sentiment slumped, spending
   growing" or its inverse), sentiment level, real PCE YoY, rendered from status.
2. Tiles: sentiment (with percentile-flavoured note vs history), real PCE YoY, real
   retail YoY, saving rate, and the computed "spending contractions since 1960" count
   with its median lag.
3. Main chart: sentiment level, 1952 →, bands. Seventy years of mood.
4. The sentiment slumps table with rule printed and computed footnotes (newest row
   from data; 2011 and the current mega-slump named as the false positives; the
   quarterly-before-1978 basis note).
5. What they do: real PCE YoY (1960 →) and real retail YoY (1993 →) on one chart,
   zero line, bands; the contraction lag table beneath with its own printed rule and
   the computed median-lag sentence.
6. Income and the buffer: real disposable income YoY and the saving rate, duo cards,
   1959/1960 →. A 3% saving rate means spending is running ahead of income growth;
   stated from tokens, not asserted.
7. Canada: household consumption YoY quarterly (1962 →), the retail pair (closed
   1991-2022 and successor 2017 →, overlap visible, never spliced), C.D. Howe bands.
8. Canada expectations mini-card: CSCE 1-year expectations and perceptions (2014 →),
   with the no-free-sentiment-index gap stated.
9. Revisions card (busy, says why), sources card with every series, the terminated
   cube, the successor coordinates, fetch policy, econ-core note, provenance line.

## Deploy checklist (identical to freight's, values changed)

Port 8135; `sites/consumer.caddy`; services.yml entry with dashy + kuma blocks;
`cf-access.sh create consumer.chrislawrence.ca --policy public` + retry; cron +
truncation; screenshots (mobile fullPage, desktop, both tables) + layout audit +
console check; `ls -l data/`; commit; push private `Lawrence908/consumer`.

## Anti-goals

- No splicing the retail survey redesign, the sentiment cadence change, or anything
  else; seams are shown.
- No house "consumer health index", no percentile gauges beyond plain computed
  comparisons, no deflating anything ourselves (real series come from their
  publishers).
- No spinning the sentiment false positives; the say-do gap is the content.
- No forecasts, no emdashes in page copy.

## Acceptance

- All series land with zero errors on a cold start, contract-validated, including
  both Canadian retail vectors (one from an inactive cube) with live title checks.
- The sentiment table reproduces the canonical record (slumps into every recession
  since 1969, 2011 and 2021+ as false positives) or the discrepancy is investigated
  until the table is believed; rules then frozen and printed.
- The contraction table computes a positive median lag or the page honestly reports
  whatever sign it shows.
- Kill `FRED_API_KEY`: everything still refreshes.
- Both containers healthy, public 200, Kuma green, screenshots committed, zero
  console errors, no horizontal scroll, repo pushed, no machine-owned files in git.
