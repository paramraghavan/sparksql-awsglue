# Petabyte-Scale RTBF Updates

This note explains how to perform monthly RTBF, or right to be forgotten, updates over petabyte-scale analytical data. The focus is a three-year sliding window with about 5 to 10 datasets, stored today in S3, totaling about 1 PB.

RTBF in this context means:

- Redacting selected columns, such as phone, email, name, address, or device identifiers.
- Masking or tokenizing personal fields.
- Marking some rows as logically deleted because the customer is no longer active.
- Rarely physically purging rows when law, contract, or retention rules require it.

The monthly process handles the full retained window:

```text
Window size:      3 years, about 1,097 days
Data size:        about 1 PB total
Datasets:         about 5 to 10 S3 datasets
Monthly pattern:  add current month, expire oldest month
RTBF cadence:     run once per month
Primary actions:  redact, mask, logical delete
Rare action:      physical purge
```

---

## Short Recommendation

Best default solution for cost, ease of use, and maintainability:

```text
Use S3 as the durable source of truth.
Store lake data as Iceberg or Hudi tables, not plain loose Parquet if RTBF is common.
Use EMR Spark or EMR Serverless Spark for the monthly RTBF update job.
Use Glue Catalog for table metadata.
Use Athena or Redshift Spectrum for verification and ad hoc reads.
Use Redshift, Snowflake, or StarRocks only as serving/query layers where needed.
```

In simple terms:

```text
S3 + Iceberg/Hudi + Glue + EMR Spark
  -> best core architecture for monthly RTBF over 1 PB

Snowflake or StarRocks
  -> good serving/query engines
  -> not the cheapest place to store and rewrite the full 1 PB every month

Redshift + Glue/Spectrum
  -> good for querying S3 and BI marts
  -> not the best engine for rewriting 1 PB of lake data
```

If choosing only one main processing engine for the monthly RTBF operation, choose **EMR Spark over S3 lake tables**.

---

## Why EMR Spark Is the Best Default

RTBF over 1 PB is a large batch data engineering problem. Spark is designed for this kind of distributed file/table processing.

Use EMR Spark when:

- The data already lives in S3.
- There are multiple datasets.
- The job runs monthly.
- The operation is mostly row/column updates, redaction, masking, and logical deletes.
- Cost matters.
- You need control over partitions, files, table formats, and retries.

Recommended layout:

```text
S3 lake
  -> raw zone, optional immutable source
  -> curated zone, Iceberg/Hudi tables
  -> redacted zone or updated table snapshots

Glue Catalog
  -> table metadata
  -> partitions and schemas

EMR Spark
  -> monthly RTBF batch
  -> redaction/masking/deletion logic
  -> compaction
  -> snapshot publishing

Athena / Redshift Spectrum
  -> verification queries
  -> historical reads

Redshift / Snowflake / StarRocks
  -> hot serving tables, summaries, dashboards
```

---

## Monthly RTBF Flow

The monthly job should not be a random collection of scripts. Treat it as a controlled batch with an auditable request set.

```text
1. Freeze the monthly RTBF batch
   -> collect approved customer requests, legal rules, and business rules
   -> assign a batch id, for example rtbf_batch=2026-10
   -> normalize keys: customer_id, account_id, phone_hash, email_hash, device_id

2. Classify the action
   -> redact selected columns
   -> mask selected columns
   -> set is_deleted=true
   -> physically purge only when required

3. Process the three-year window
   -> read affected datasets
   -> apply RTBF rules
   -> update Iceberg/Hudi tables or rewrite affected partitions
   -> compact files if needed

4. Refresh serving layers
   -> update StarRocks tables if used
   -> update Snowflake tables if used
   -> update Redshift marts/materialized views if used

5. Rebuild summaries
   -> last 90 days can stay detailed and hot
   -> older data can be stored as monthly/customer/product summaries
   -> rebuild only summaries affected by the RTBF batch

6. Verify and audit
   -> query lake tables, Athena, Redshift, Snowflake, and StarRocks as applicable
   -> confirm redacted fields are gone
   -> confirm logically deleted customers are hidden
   -> store counts, hashes, batch id, and completion status
```

---

## Redaction vs Logical Delete vs Purge

Do not physically delete everything by default. Most RTBF and business-rule cases are easier and cheaper as redaction or logical deletion.

```text
Redaction
  -> replace sensitive column values with null, masked value, or token
  -> keeps non-personal analytical facts usable
  -> best for reporting continuity

Logical delete
  -> keep the row but set is_deleted=true or deleted_at=batch_time
  -> dashboards and downstream jobs filter it out
  -> useful when the row must not appear but audit counts matter

Physical purge
  -> remove matching rows from the lake and serving systems
  -> highest operational cost
  -> use when law, contract, or retention policy requires removal
```

Example:

```text
Customer left the business:
  -> set is_deleted=true in customer/profile tables
  -> remove customer from active dashboards
  -> keep non-personal aggregated financial facts if allowed

Privacy request:
  -> redact name, phone, email, address, device ids
  -> mask customer identifiers where required
  -> purge only datasets where policy requires full removal
```

---

## Handling Old S3 Files, Versions, and Prefixes

For RTBF, deleting or rewriting only the latest visible dataset is not enough. Old S3 files can still exist in previous prefixes, noncurrent S3 object versions, lakehouse snapshots, backup locations, or downstream serving layers.

Important S3 detail:

```text
S3 does not have real folders.
An S3 prefix is just part of the object key.
There is no atomic "rename prefix" operation.
```

So "quarantine the old prefix" usually does **not** mean renaming a folder. It means one of these patterns:

```text
Preferred:
  stop referencing the old prefix
  switch readers to the clean prefix
  block normal access to the old prefix
  automatically delete the old prefix after the safety window

Optional but expensive:
  copy old objects to _quarantine/...
  delete old active objects
  treat this as a physical move, because S3 move = copy + delete

Alternative:
  tag old objects as quarantined
  use lifecycle rules by tag to delete them automatically
```

Use this rule of thumb:

```text
Do not update S3 files in place.
Write clean files to a new prefix.
Switch readers to the clean prefix.
Quarantine old data logically unless physical movement is required.
Use S3 Lifecycle or a purge job for automatic deletion.
Purge old current objects, noncurrent versions, and stale metadata after the safety window.
```

Recommended workflow:

```text
1. Identify affected files and partitions
   -> find all S3 prefixes, table snapshots, and downstream copies containing the subject

2. Rewrite clean data to a new prefix
   -> exclude or redact the RTBF subject
   -> write immutable Parquet/Iceberg/Hudi output

3. Switch readers atomically
   -> update Iceberg/Hudi snapshot, Glue table location, manifest, or pipeline config
   -> make Athena, Spark, Snowflake, Redshift Spectrum, and serving layers read only clean data

4. Quarantine old data
   -> usually do not rename or move the S3 prefix
   -> remove old references from active catalogs
   -> deny normal analytics access to the old prefix
   -> optionally tag old objects as quarantined
   -> optionally keep old files for 7 days for rollback if policy allows

5. Final purge
   -> delete current objects
   -> delete noncurrent object versions if S3 Versioning is enabled
   -> remove expired delete markers
   -> expire old Iceberg/Hudi snapshots that still reference old files

6. Verify and audit
   -> use S3 Inventory, ListObjectVersions, table metadata, and query checks
   -> record request id, batch id, old prefix, new prefix, purge time, and verification result
```

Example prefix pattern:

```text
Old active prefix:
s3://company-lake/curated/orders/date=2026-10-10/

Clean rewritten prefix:
s3://company-lake/curated/orders_rtbf/run_id=rtbf_2026_10/date=2026-10-10/

Quarantine prefix:
s3://company-lake/_quarantine/rtbf/orders/run_id=rtbf_2026_10/date=2026-10-10/
```

Use the quarantine prefix only when you truly need to physically group old files under `_quarantine/`. At petabyte scale, copying old objects to a quarantine prefix can be expensive, slow, and unnecessary. The cheaper pattern is:

```text
old prefix remains physically where it is
catalog/table pointer moves to the clean prefix
IAM blocks normal reads from the old prefix
lifecycle or purge job deletes the old prefix after 7 days
```

Seven-day safety window pattern:

```text
Day 0:
  rewrite clean data
  publish clean table snapshot/pointer
  remove old prefix from active reads
  register old prefix for purge
  optionally tag old objects with rtbf_status=quarantine

Days 1-7:
  monitor downstream jobs
  allow controlled rollback only through restricted admin process
  keep old prefix inaccessible to normal analytics users

Day 7:
  lifecycle or purge job deletes old current objects
  purge noncurrent S3 versions
  remove expired delete markers
  expire old lakehouse snapshots
  store audit proof
```

Important S3 Versioning detail:

```text
In a versioned S3 bucket, a normal delete can create a delete marker.
Older object versions may still exist.
For RTBF purge, configure lifecycle rules or an explicit purge job for noncurrent versions.
```

Automatic deletion options:

```text
Option A: Lifecycle by quarantine prefix
  Prefix: _quarantine/rtbf/
  Expire current versions: 7 days
  Permanently delete noncurrent versions: 7 days
  Delete expired delete markers: enabled

Option B: Lifecycle by old data prefix
  Prefix: curated/orders/date=2026-10-10/
  Expire current versions: 7 days
  Permanently delete noncurrent versions: 7 days
  Delete expired delete markers: enabled

Option C: Lifecycle by object tag
  Tag: rtbf_status=quarantine
  Tag: purge_after=2026-10-18
  Expire current versions according to lifecycle policy
  Permanently delete noncurrent versions according to lifecycle policy
```

A purge-control table is useful for audit and automation:

```text
rtbf_batch_id
dataset_name
old_prefix
new_prefix
quarantine_started_at
purge_after_at
quarantine_mode       -- pointer_only, tag_only, physical_move
purge_status          -- pending, running, complete, failed
verification_status   -- pending, passed, failed
```

If S3 Object Lock, legal hold, or compliance-mode retention is enabled, the files might not be deletable until the retention period expires. That is a compliance and retention-policy decision, not just a Spark/S3 engineering issue.

Lakehouse table formats also need cleanup:

```text
Iceberg:
  -> publish clean snapshot
  -> expire old snapshots after allowed rollback window
  -> remove orphan files after snapshots no longer reference them

Hudi:
  -> commit clean write/delete
  -> run cleaner according to retention policy
  -> verify old file slices are no longer retained beyond the safety window

Plain Parquet:
  -> switch Glue/Athena/Snowflake/Redshift Spectrum pointers to clean prefix
  -> remove old prefix from active readers
  -> optionally tag old objects or copy to quarantine prefix
  -> lifecycle/purge old versions
```

Checklist:

- [ ] Find every active and backup prefix containing the subject.
- [ ] Rewrite clean files to a new prefix.
- [ ] Publish clean table/catalog pointer.
- [ ] Remove old prefix from active readers.
- [ ] Choose quarantine mode: pointer-only, tag-only, or physical copy+delete.
- [ ] Deny normal analytics access to old/quarantined data.
- [ ] Create lifecycle rules or purge jobs for old prefixes/tags.
- [ ] Purge current and noncurrent S3 object versions.
- [ ] Remove expired delete markers.
- [ ] Expire old Iceberg/Hudi snapshots.
- [ ] Verify with S3 Inventory or object-version listing.
- [ ] Store audit evidence for the RTBF request.

---

## Best Table Format

Plain Parquet works, but it is harder to maintain when updates and deletes are repeated every month.

Better choices:

```text
Apache Iceberg
  -> strong option for S3 lakehouse tables
  -> supports snapshots, deletes, updates/merge patterns through Spark
  -> works with Glue Catalog and many query engines
  -> good for monthly RTBF with audit and rollback

Apache Hudi
  -> strong option for upserts and incremental processing
  -> good when record-level changes are common
  -> more operational choices to understand

Plain Parquet
  -> cheapest and simplest file format
  -> but updates require partition rewrites
  -> acceptable if RTBF volume is small and monthly rewrite is manageable
```

Recommended:

```text
Use Iceberg if starting fresh or modernizing the lake.
Use Hudi if upserts/deletes are frequent and the team already knows Hudi.
Use plain Parquet only if the process is simple and mostly partition rewrites.
```

Important limitation:

```text
Iceberg/Hudi reduce operational pain.
They do not remove the physical cost of changing data that must be rewritten.
```

Impact-size guide:

```text
Small RTBF impact
  -> a few customers or records touch a limited number of files/partitions
  -> Iceberg/Hudi are very useful
  -> update/delete/redact only affected files or table snapshots

Medium RTBF impact
  -> many customers across many months, but not every file
  -> Iceberg/Hudi still help with metadata, snapshots, rollback, and publishing
  -> Spark may still rewrite many files or partitions

Large RTBF impact
  -> almost every file or almost every row changes
  -> treat it as a controlled full rebuild
  -> Spark reads the old table, applies RTBF rules, and writes a new clean snapshot
  -> Iceberg/Hudi help with atomic publish and rollback, but cost is close to full rewrite
```

Examples:

```text
Good Iceberg/Hudi win:
  1,000 customers need redaction across a 1 PB lake.
  Only files/partitions containing those customers need updates.

Full-rewrite case:
  A business rule says redact one column for every row in every dataset.
  That physically changes almost all files, so the monthly job is close to a 1 PB rewrite.
```

---

## Option Comparison

### Option 1: EMR Spark over S3

This is the best default for the monthly RTBF job.

```text
Architecture:
  S3 Iceberg/Hudi/Parquet
    -> Glue Catalog
    -> EMR Spark monthly RTBF job
    -> cleaned table snapshots in S3
    -> Athena/Spectrum verification
    -> optional serving refresh to Redshift/Snowflake/StarRocks
```

Pros:

- Best cost profile for 1 PB lake processing.
- Keeps S3 as durable source of truth.
- Handles 5 to 10 datasets naturally.
- Good for redaction, masking, logical deletes, and rare purges.
- Open architecture with less vendor lock-in.
- Easy to keep backup/history through table snapshots and S3 versioning/lifecycle.

Cons:

- Requires Spark engineering skill.
- Needs careful partitioning, compaction, retries, and monitoring.
- Plain Parquet rewrites can be expensive if partitions are poorly designed.

Best use:

```text
Monthly RTBF over full 3-year S3 data window.
Large table rewrites or Iceberg/Hudi updates.
Batch compliance processing.
```

Verdict:

```text
Best overall choice for cost + control + maintainability.
```

---

### Option 2: Snowflake

Snowflake is easy to use, but storing and updating the full 1 PB in Snowflake every month can be expensive.

Two possible Snowflake patterns:

```text
Pattern A: Snowflake as primary store
  -> load all 1 PB into Snowflake
  -> run RTBF updates/deletes in Snowflake
  -> use S3 only as backup/export

Pattern B: S3 as primary store, Snowflake as serving/query layer
  -> keep full 1 PB in S3
  -> process RTBF in EMR Spark
  -> refresh only hot/query tables in Snowflake
```

Pros:

- Very easy SQL experience.
- Strong governance and access controls.
- Good for analyst productivity.
- Updates/deletes are simpler from the user perspective.

Cons:

- Full 1 PB storage plus monthly update compute can be expensive.
- Large updates can rewrite micro-partitions internally and consume warehouse credits.
- If S3 remains only backup, the operational source of truth moves into Snowflake.
- Less cost-efficient than Spark/S3 for full-window lake rewrites.

Best use:

```text
Hot or warm serving tables.
Curated marts.
Analytics where ease of SQL is worth the cost.
```

Verdict:

```text
Use Snowflake for serving and analytics.
Avoid making it the only engine for monthly 1 PB RTBF unless budget and simplicity matter more than cost.
```

---

### Option 3: StarRocks

StarRocks is strong for fast analytical serving and mutable tables, but it is usually not the cheapest system for storing all historical raw detail if S3 can do that.

Two possible StarRocks patterns:

```text
Pattern A: StarRocks as primary analytical store
  -> load detailed data into StarRocks
  -> update/delete/mask rows there
  -> keep S3 as backup

Pattern B: S3 as primary lake, StarRocks as hot serving layer
  -> process full RTBF in S3 with EMR Spark
  -> load last 90 days or summaries into StarRocks
  -> apply fast serving updates in StarRocks
```

Pros:

- Excellent dashboard/query performance.
- Good for high-concurrency analytics.
- Supports mutable serving patterns such as primary-key/update style tables.
- Useful when RTBF must become visible quickly in user-facing analytics.

Cons:

- Operating another distributed database adds maintenance.
- Storing the full 1 PB there may be more expensive than S3.
- S3 backup still needs its own RTBF cleanup if it contains personal data.
- Not the best default for full historical lake rewrite.

Best use:

```text
Last 90 days of hot detail.
Customer/profile serving tables.
Fast dashboards.
Precomputed summaries.
Fast visibility of redaction/deletion results.
```

Verdict:

```text
Use StarRocks as a serving layer, not the main monthly 1 PB processing engine.
```

---

### Option 4: Redshift, Glue, and Spectrum

Glue and Redshift Spectrum are useful for metadata and querying S3, but Spectrum is not a replacement for Spark.

Important:

```text
Glue Catalog
  -> metadata layer, not a processing engine

Redshift Spectrum
  -> query S3 external tables from Redshift
  -> uses AWS-managed Spectrum workers
  -> does not use your EMR cluster

Redshift managed tables
  -> data stored inside Redshift
  -> queries run on Redshift provisioned nodes or Redshift Serverless RPUs
```

Pros:

- Good fit if the team already uses Redshift for BI.
- Spectrum can query cleaned S3 data without loading all history into Redshift.
- Glue Catalog provides shared metadata for Athena, Spectrum, Spark, and Glue.

Cons:

- Spectrum is for querying, not heavy monthly table rewrites.
- Large updates in Redshift managed tables are not ideal for the full 1 PB history.
- Redshift can become expensive if used as both storage and processing layer for all history.

Best use:

```text
BI marts and dashboards.
Materialized views and hot aggregates.
Verification queries over S3 external tables.
Joining Redshift dimensions with S3 historical facts.
```

Verdict:

```text
Use Glue + Spectrum + Redshift for query and serving.
Use EMR Spark for the monthly RTBF processing.
```

---

## Best Architecture

Recommended architecture for your case:

```text
Source of truth:
  S3 lake with Iceberg or Hudi tables

Metadata:
  Glue Catalog

Monthly RTBF processing:
  EMR Spark or EMR Serverless Spark

Verification:
  Athena and/or Redshift Spectrum

Hot serving:
  StarRocks, Redshift, or Snowflake for last 90 days and summaries

Backup/archive:
  S3 versioning, table snapshots, lifecycle policies, Glacier where appropriate
```

Data flow:

```text
Monthly RTBF request batch
  -> normalized redaction/deletion keyset
  -> EMR Spark reads 5 to 10 lake tables
  -> apply redaction, masking, logical delete, rare purge
  -> write new Iceberg/Hudi snapshots or rewritten partitions
  -> refresh Glue metadata
  -> refresh hot serving tables and summaries
  -> verify in Athena/Spectrum/Snowflake/StarRocks/Redshift
  -> publish audit report
```

---

## Hot 90 Days plus Historical Summaries

This pattern gives a good balance between cost and performance.

```text
Last 90 days:
  -> detailed data in StarRocks, Redshift, or Snowflake
  -> fast dashboard queries
  -> fast RTBF visibility

Older than 90 days:
  -> detailed data remains in S3 lake
  -> monthly/customer/product summaries loaded into serving engine
  -> raw detail queried only when needed
```

Dashboard example:

```text
Dashboard result =
  last 90 days detail from hot serving table
  +
  older history from precomputed monthly summaries
```

RTBF impact:

```text
If RTBF affects last 90 days:
  -> update hot detail table
  -> rebuild recent summaries

If RTBF affects older data:
  -> update S3 lake table
  -> rebuild affected monthly summaries
  -> refresh serving copy
```

---

## Processing the Full Three-Year Window Monthly

If policy says the monthly RTBF job must process the full retained window, design it so the full scan is controlled and auditable.

```text
1. Add current month to the window.
2. Expire oldest month from the window.
3. Read RTBF keyset and rules for the month.
4. Process all retained months across 5 to 10 datasets.
5. Rewrite only changed partitions when possible.
6. Rebuild affected summaries.
7. Verify redaction/deletion results.
8. Publish audit report.
```

Optimization:

```text
Full-window evaluation does not always mean full-window rewrite.

Scan or validate the full retained window if required.
Rewrite only the partitions, files, or table snapshots that actually changed.
```

For 1 PB, this distinction is important. A monthly full read may be acceptable with a tuned Spark cluster. A monthly full rewrite of 1 PB should be avoided unless the business or legal requirement truly demands it.

Decision rule:

```text
If RTBF changes a small portion of files:
  -> use Iceberg/Hudi update/delete/merge patterns
  -> rewrite only affected files or partitions

If RTBF changes most files:
  -> use Spark as a full rebuild job
  -> write a new clean table snapshot
  -> use Iceberg/Hudi for atomic publish, audit, and rollback
```

---

## Cost and Maintenance Ranking

For the monthly 1 PB RTBF update itself:

| Rank | Option | Cost | Ease | Maintenance | Recommendation |
|---:|---|---|---|---|---|
| 1 | EMR Spark + S3 Iceberg/Hudi + Glue | Best | Medium | Medium | Best default |
| 2 | EMR Serverless or Glue ETL + S3 | Good | Easier | Lower | Good if jobs fit managed limits |
| 3 | Snowflake primary store | High | Easiest | Low | Use if simplicity beats cost |
| 4 | StarRocks primary store | Medium/High | Medium | Medium/High | Better as serving layer |
| 5 | Redshift/Spectrum as processor | Medium/High | Medium | Medium | Use for query, not main RTBF rewrite |

Best practical combination:

```text
Core RTBF:
  EMR Spark + S3 Iceberg/Hudi + Glue

Query verification:
  Athena or Redshift Spectrum

Hot analytics:
  StarRocks or Redshift or Snowflake

Backup:
  S3 snapshots, lifecycle policies, Glacier
```

---

## Final Recommendation

For your monthly RTBF workload over 1 PB and 5 to 10 datasets, use:

```text
S3 as source of truth
Iceberg or Hudi table format for update/delete support
Glue Catalog for metadata
EMR Spark for monthly RTBF processing
Athena/Redshift Spectrum for verification
StarRocks, Snowflake, or Redshift only for hot serving and summaries
S3 snapshots/lifecycle policies for backup and retention
```

Do not make Snowflake or StarRocks the only copy just to avoid S3 processing unless the team intentionally accepts higher storage/compute cost for simpler operations. S3 is still the best long-term storage layer for 1 PB. EMR Spark is the best default engine for monthly petabyte-scale RTBF updates.
