# Daily Financial Summary: 2024-11-13

## Headline KPIs
| KPI | Value |
|---|---|
| Revenue on 2024-11-13 | $792,421 |
| Revenue month to date | $3,117,326 |
| Gross margin month to date | $1,167,353 (37.4%) |
| AR over 90 days | $0 of $11,730,546 open |
| Data quality pass rate | 99.19% (8 of 992 rows quarantined) |

## Top anomalies
1. Revenue for ENT-03 on 2024-11-13 was $202,980, about 6.55x the trailing 28-day average ($30,980). Z-score 10.8307.
2. Revenue for ENT-01 on 2024-11-13 was $254,938, about 4.99x the trailing 28-day average ($51,053). Z-score 5.1365.
3. Revenue for ENT-02 on 2024-11-13 was $197,138, about 5.28x the trailing 28-day average ($37,360). Z-score 6.263.

## Top 5 month-over-month movers (November 2024)
| Rank | Account | Type | This month | Prior month | Change | Change % |
|---|---|---|---|---|---|---|
| 1 | 1200 Inventory | asset | $-1,939,426 | $-2,670,353 | $730,927 | 27.4% |
| 2 | 4012 Revenue 13 | revenue | $-80,816 | $-247,019 | $166,203 | 67.3% |
| 3 | 4018 Revenue 19 | revenue | $-119,382 | $-238,580 | $119,197 | 50.0% |
| 4 | 5007 Cost of Goods Sold 08 | cogs | $162,754 | $280,646 | $-117,892 | -42.0% |
| 5 | 1100 Accounts Receivable | asset | $2,351,447 | $2,241,660 | $109,787 | 4.9% |

---

## Technical appendix
Batch `2024-11-13-4dfe525a`

### Stage timings and row counts
| Stage | Status | Rows in | Rows out | Seconds |
|---|---|---|---|---|
| ingest | skipped |  |  | 0.0 |
| transform | success | 992 | 992 | 0.19 |
| validate | success | 992 | 984 | 1.7 |
| load | success | 984 | 0 | 0.03 |

### Reconciliation
raw 992 rows = curated 984 + quarantined 8 + duplicates removed 0 (gap 0); amount gap 0.0. **Reconciled: True**

### Quarantine reasons
| Rule | Rows |
|---|---|
| DQ-003 | 3 |
| DQ-015 | 2 |
| DQ-001 | 1 |
| DQ-006 | 1 |
| DQ-011 | 1 |
| DQ-008 | 1 |
| DQ-010 | 1 |


### Rules that did not pass
| Rule | Severity | Rows failed | Rate | Description |
|---|---|---|---|---|
| DQ-003 | blocking | 3 | 0.0030 | amount_local must be present |
| DQ-015 | blocking | 2 | 0.0020 | FX rate must resolve for every row (unknown currency or missing rate) |
| DQ-008 | blocking | 1 | 0.0010 | posted_date must be between 2000-01-01 and the day after the run date |
| DQ-010 | blocking | 1 | 0.0010 | account_id must exist in dim_account |
| DQ-011 | blocking | 1 | 0.0010 | entity_id must exist in dim_entity |
| DQ-001 | blocking | 1 | 0.0010 | transaction_id must be present on every row |
| DQ-006 | blocking | 1 | 0.0010 | currency must be one of the governed ISO codes |
| DQ-017 | info | 1 | 0.0010 | description should not be blank |
| DQ-016 | warning | 2 | 0.0020 | credit-normal accounts (revenue, liability, equity) must not carry positive debit amounts on sale events |
| DQ-012 | warning | 1 | 0.0010 | customer_id should exist in dim_customer (current version) |
