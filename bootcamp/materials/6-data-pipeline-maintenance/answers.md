# Pipeline Management Plan

## Team
We are a team of 4 Data Engineers responsible for the monitoring, upkeep, and reporting for the following investor-facing and experimental pipelines:

### Covered Pipelines
1. Unit-Level Profit (for experiments)
2. Aggregate Profit (for investors)
3. Aggregate Growth (for investors)
4. Daily Growth (for experiments)
5. Aggregate Engagement (for investors)

---

## Ownership Plan

| Pipeline                         | Primary Owner     | Secondary Owner       |
|----------------------------------|-------------------|------------------------|
| Unit-Level Profit                | Data Engineer 1   | Finance / Risk Team    |
| Aggregate Profit                 | Data Engineer 2   | Finance / Risk Team    |
| Aggregate Growth                 | Data Engineer 3   | Accounts Team          |
| Daily Growth                     | Data Engineer 4   | Accounts Team          |
| Aggregate Engagement             | Data Engineer 1   | Frontend SWE Team      |

---

## On-Call Schedule (Fair Rotation + Holidays)

We rotate on-call responsibilities weekly, ensuring that all 5 pipelines are covered. Since there are 4 engineers and 5 pipelines, each week, one engineer will handle 2 pipelines (typically paired logically, e.g., Profit-related tasks).

### Weekly Assignment (Starting Aug 5, 2025)

| Week # | Dates (Mon–Sun)     | Engineer Assignments                                       |
|--------|---------------------|-------------------------------------------------------------|
| 1      | Aug 4 – Aug 10      | DE1: Unit Profit + Engagement<br>DE2: Aggregate Profit<br>DE3: Aggregate Growth<br>DE4: Daily Growth |
| 2      | Aug 11 – Aug 17     | DE2: Unit Profit + Engagement<br>DE3: Aggregate Profit<br>DE4: Aggregate Growth<br>DE1: Daily Growth |
| 3      | Aug 18 – Aug 24     | DE3: Unit Profit + Engagement<br>DE4: Aggregate Profit<br>DE1: Aggregate Growth<br>DE2: Daily Growth |
| 4      | Aug 25 – Aug 31     | DE4: Unit Profit + Engagement<br>DE1: Aggregate Profit<br>DE2: Aggregate Growth<br>DE3: Daily Growth |

Repeat the 4-week cycle going forward.

### Holiday Policy

- Swap weeks in advance if holidays or PTO fall on your assigned on-call window
- Each engineer may designate 1 “skip” week per quarter for vacation or other planned leave
- The final week of each month will have elevated monitoring for investor reports; all DEs are expected to assist

---

## Runbooks for Investor-Facing Pipelines

### Pipeline: Unit-Level Profit (for Experiments)

**Data Used:**
- Revenue by individual subscriber
- Per-account cost breakdown (infra, support, etc.)

**Potential Issues:**
- Data missing or misaligned at the subscriber level
- Delay in receiving cost center breakdowns
- Inconsistent unit ID formats across datasets

**SLA:**
- Updated daily or weekly for experiment team analysis
- No hard on-call, but monitored by assigned DE

---

### Pipeline: Aggregate Profit (for Investors)

**Data Used:**
- Total revenue across accounts
- Total costs from Ops (infra, salaries, services)

**Potential Issues:**
- Revenue mismatch with filings
- Cost reports updated late
- Exchange rate issues for international accounts

**SLA:**
- Must be clean and accurate before end-of-month report
- Reviewed monthly by Finance

---

### Pipeline: Aggregate Growth (for Investors)

**Data Used:**
- Total active accounts
- Renewals, upgrades, and churn

**Potential Issues:**
- Status change steps skipped (e.g., B missing in A→B→C flow)
- Incorrect subscription flags
- Time series gaps

**SLA:**
- Refreshed weekly
- Final data validated by end of month

---

### Pipeline: Daily Growth (for Experiments)

**Data Used:**
- Account-level changes logged daily
- Licenses added or removed per account

**Potential Issues:**
- Incomplete daily logs
- Missing timestamped snapshots
- Lag from AE team updating CRM

**SLA:**
- Must be updated daily
- No hard on-call, but assigned engineer is responsible for debugging if needed

---

### Pipeline: Aggregate Engagement (for Investors)

**Data Used:**
- Clicks, session time per user
- Aggregated per-account daily usage

**Potential Issues:**
- Late-arriving Kafka events
- Duplicate clicks
- Kafka downtime leading to data gaps
- Incomplete joins for company-level aggregations

**SLA:**
- Latest event timestamp must be within 48 hours
- Fix issues within 1 week
- Final aggregates must be correct by month-end

---

## Final Notes

- We hold a 30-minute weekly handoff on Fridays for context transfer
- Documentation and ownership lists will be updated quarterly
- Data Engineers will collaborate with Data Science and Business Analytics teams to support investor and experiment pipelines
