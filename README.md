# Apache Iceberg Data Warehouse Dissection

A practical deep-dive into how Apache Iceberg works internally. This project demonstrates the underlying architecture of Apache Iceberg through a simple e-commerce ETL pipeline, focusing on **metadata management**, **snapshots**, and the data warehouse structure.

> This is a dissection project designed to understand Apache Iceberg from the ground up - showing how metadata tracking, versioning, and ACID transactions work in a real-world context.

---

## 📋 Project Intent & Approach

### The Dissection Goal

This project was created to understand Apache Iceberg from first principles - specifically how Iceberg differs from Hadoop/Hive and why **metadata is the key innovation**.

### The Simple ETL

The ETL intentionally keeps things simple to focus on Iceberg:
- No cloud integration (uses local Hadoop warehouse)
- No complex partitioning strategies
- No watermarking or incremental loads
- No advanced modularization

This simplicity is **intentional** - the focus is on understanding Iceberg's metadata layer, not production DE patterns.

### The Data

Generated to mimic OLTP extracts with intentional inconsistencies:
- Multiple date formats (YYYY-MM-DD, DD-MM-YYYY, YYYY/MM/DD)
- Null values and missing fields
- Real-world data quality issues

This demonstrates transformation logic while keeping the ETL code readable.

### The Iceberg Setup

Required careful configuration of Spark and Iceberg JARs. This complexity is intentional - it shows the machinery behind Iceberg and makes the metadata system visible and understandable.

---

## 🏗️ Apache Iceberg: Data vs Metadata

Apache Iceberg fundamentally changes how data warehouses work by separating concerns into **two distinct layers**:

```
┌────────────────────────────────────────────────────────────┐
│               Apache Iceberg Table                         │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  ┌──────────────────────────────────────────────────────┐ │
│  │           METADATA LAYER (The Innovation)           │ │
│  │                                                      │ │
│  │  - Tracks all versions (snapshots)                  │ │
│  │  - Stores schema and partitioning info              │ │
│  │  - Manages file listings and statistics             │ │
│  │  - Enables time-travel and ACID transactions        │ │
│  │  - Provides isolated views for concurrent readers   │ │
│  └──────────────────────────────────────────────────────┘ │
│                        ▲                                   │
│                        │ (JSON & Avro files)              │
│                        ▼                                   │
│  ┌──────────────────────────────────────────────────────┐ │
│  │           DATA LAYER (Parquet files)                │ │
│  │                                                      │ │
│  │  - Actual table data in Parquet format              │ │
│  │  - Partitioned across multiple files                │ │
│  │  - Immutable once written                           │ │
│  └──────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────┘
```

### Data Layer

**What:** Actual table data stored in Parquet format  
**Where:** `datawarehouse/dw_[entity]/data/` directory  
**Immutability:** Once written, never modified  

```
dw_orders/data/
├── partition_date=2026-04-25/
│   ├── 00000-1-d3fb5e49-1234.parquet
│   └── 00001-1-e4fc5f50-5678.parquet
└── partition_date=2026-04-26/
    └── 00000-1-f5gd6g51-9012.parquet
```

### Metadata Layer

**What:** Files that track, manage, and version the data  
**Where:** `datawarehouse/dw_[entity]/metadata/` directory  
**Structure:** JSON and Avro files that form a versioning system  

```
dw_orders/metadata/
├── v1.metadata.json (root)
├── snap-9056477531426576452-1-72f25b2a.avro
├── 72f25b2a-0edf-4a81-9bf3-d66d36108d5a-m0.avro
├── version-hint.text
├── .v1.metadata.json.crc
└── ... (more snapshots and manifests)
```

---

## 📁 Metadata File System Deep Dive

This is the **core** of Iceberg. Understanding these files is understanding Iceberg itself.

### 1. vX.metadata.json - Root Metadata File

**The most important file.** This is the entry point to everything.

**Location:** `datawarehouse/dw_orders/metadata/v1.metadata.json`

**Purpose:**
- Root file for the entire Iceberg table
- Contains complete table definition
- Points to current snapshot
- Stores all metadata history

**Structure:**

```json
{
  "format-version": 1,
  
  "schema": {
    "type": "struct",
    "fields": [
      { "id": 1, "name": "order_id", "type": "string", "required": true },
      { "id": 2, "name": "customer_id", "type": "string", "required": true },
      { "id": 3, "name": "order_date", "type": "date", "required": false },
      { "id": 4, "name": "total_amount", "type": "decimal(10,2)", "required": false },
      { "id": 5, "name": "status", "type": "string", "required": false }
    ]
  },
  
  "current-snapshot-id": 9056477531426576452,
  
  "snapshots": [
    {
      "snapshot-id": 9056477531426576452,
      "timestamp-ms": 1682419200000,
      "summary": {
        "operation": "append",
        "spark.app.id": "app-20240101",
        "added-data-files": "2",
        "added-records": "1500",
        "added-files-size": "524288"
      },
      "manifest-list": "snap-9056477531426576452-1-72f25b2a.avro"
    },
    {
      "snapshot-id": 8945366420315465341,
      "timestamp-ms": 1682332800000,
      "summary": {
        "operation": "append",
        "added-data-files": "1",
        "added-records": "800"
      },
      "manifest-list": "snap-8945366420315465341-1-61e14d08.avro"
    }
  ],
  
  "partition-spec": [
    { "name": "order_date", "transform": "day", "source-id": 3 }
  ],
  
  "properties": {
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "snappy"
  }
}
```

**Key Components:**

| Component | Purpose |
|-----------|---------|
| `format-version` | Iceberg version (1 or 2) - indicates compatibility |
| `schema` | Complete table schema with field IDs, names, types |
| `current-snapshot-id` | Points to the "current" state of the table |
| `snapshots` | Array of ALL snapshots ever created (enables time-travel) |
| `partition-spec` | How data is partitioned (e.g., by date) |
| `properties` | Configuration (compression, format, etc.) |

**Why This Design?**
- Single source of truth for table definition
- All history preserved for time-travel
- Field IDs allow schema evolution (columns can be added/removed)
- Snapshot references enable point-in-time queries

---

### 2. snap-*.avro - Manifest List

**Links a snapshot to its manifest files.**

**Location:** `datawarehouse/dw_orders/metadata/snap-9056477531426576452-1-72f25b2a.avro`

**Naming Convention:** `snap-[snapshot-id]-1-[file-id].avro`

**Purpose:**
- Lists which manifest files belong to this snapshot
- One Manifest List per snapshot
- Enables snapshot isolation

**What It Contains (Avro format):**

```
manifest_path: "/app/datawarehouse/dw_orders/metadata/72f25b2a-0edf-4a81-9bf3-d66d36108d5a-m0.avro"
manifest_length: 1024
partition_spec_id: 0
content: "data"
sequence_number: 1
min_sequence_number: 1
added_snapshot_id: 9056477531426576452
added_rows: 1500
existing_rows: 0
deleted_rows: 0
partitions: [
  { values: { order_date: "2026-04-25" } }
]
```

**Key Information:**
- `manifest_path` - Points to the manifest entry file (-m0.avro)
- `added_snapshot_id` - Which snapshot this manifest belongs to
- `added_rows` - How many rows this manifest added
- `partitions` - Which partitions are in this manifest

**Why This Design?**
- Fast lookup: Don't need to read all manifest files for a snapshot
- Partition pruning: Can filter manifests by partition value
- Snapshot isolation: Each snapshot has its own manifest list

---

### 3. *-m0.avro - Manifest Entry File

**The actual file listing. This tells Iceberg which Parquet files to read.**

**Location:** `datawarehouse/dw_orders/metadata/72f25b2a-0edf-4a81-9bf3-d66d36108d5a-m0.avro`

**Purpose:**
- Lists ALL data files (Parquet) included in this snapshot
- Stores file-level statistics
- Enables predicate pushdown (skip reading unnecessary files)

**What It Contains (Avro format):**

```
[
  {
    status: 1,  # 1=added, 2=existing, 3=deleted
    snapshot_id: 9056477531426576452,
    sequence_number: 1,
    file_sequence_number: 1,
    file_path: "/app/datawarehouse/dw_orders/data/00000-1-abc123.parquet",
    file_format: "PARQUET",
    partition: { order_date: "2026-04-25" },
    file_size_in_bytes: 262144,
    record_count: 750,
    metrics: {
      "1": {  # Column 1 (order_id)
        type: "string",
        count: 750,
        size: 45000,
        lower_bound: "O000001",
        upper_bound: "O000750"
      },
      "4": {  # Column 4 (total_amount)
        type: "decimal",
        count: 750,
        size: 7500,
        lower_bound: 100.00,
        upper_bound: 50000.00
      }
    }
  },
  {
    status: 1,
    snapshot_id: 9056477531426576452,
    file_path: "/app/datawarehouse/dw_orders/data/00001-1-def456.parquet",
    file_format: "PARQUET",
    partition: { order_date: "2026-04-25" },
    file_size_in_bytes: 262144,
    record_count: 750,
    metrics: { ... }
  }
]
```

**Key Information Per File:**

| Field | Meaning |
|-------|---------|
| `status` | 1=Added, 2=Existing, 3=Deleted (tracks file lifecycle) |
| `file_path` | Exact path to the Parquet file |
| `record_count` | How many rows in this file |
| `file_size_in_bytes` | Physical size on disk |
| `metrics` | Min/max values per column (enables predicate pushdown) |
| `partition` | Partition key value (e.g., order_date) |

**Column Metrics Example for total_amount:**
- `lower_bound: 100.00` - Minimum value in this file
- `upper_bound: 50000.00` - Maximum value in this file
- Query `WHERE total_amount > 40000` can skip files where upper_bound < 40000

**Why This Design?**
- Query planner doesn't need to read every Parquet file
- File statistics enable partition and file pruning
- Status tracking supports delete operations
- Column statistics enable predicate pushdown

---

### 4. version-hint.text - Fast Pointer

**Optimization file for quick metadata discovery.**

**Location:** `datawarehouse/dw_orders/metadata/version-hint.text`

**Contents:**
```
1
```

**Purpose:**
- Single line with filename of the latest metadata version
- Fast pointer to avoid scanning directory
- Readers start here first

**How It Works:**
```
1. Reader checks version-hint.text → "v1.metadata.json"
2. Reader opens v1.metadata.json
3. Finds current-snapshot-id = 9056477531426576452
4. Finds manifest-list pointer in snapshots
5. Opens snap-9056477531426576452-1-72f25b2a.avro
6. Gets manifest entry file paths
7. Opens *-m0.avro files
8. Gets list of Parquet files to read
```

---

### 5. .*.crc - CRC Checksums

**Data integrity validation files.**

**Location:** `datawarehouse/dw_orders/metadata/.v1.metadata.json.crc`

**Purpose:**
- CRC32 checksum for each metadata file
- Detects file corruption
- Validates file integrity during storage/transmission

**Example:**
- Original file: `v1.metadata.json`
- Checksum file: `.v1.metadata.json.crc`

---

## 🎯 How Metadata Enables Key Features

### Time-Travel Queries

Each snapshot preserves the entire state of the table at that moment:

```json
"snapshots": [
  {
    "snapshot-id": 9056477531426576452,
    "timestamp-ms": 1682419200000,
    "manifest-list": "snap-9056477531426576452-1-72f25b2a.avro"
  },
  {
    "snapshot-id": 8945366420315465341,
    "timestamp-ms": 1682332800000,
    "manifest-list": "snap-8945366420315465341-1-61e14d08.avro"
  }
]
```

**Query:** `SELECT * FROM dw_orders FOR SYSTEM_TIME AS OF '2023-04-24 10:00:00'`

**Important Note on Timestamps:**
- `timestamp-ms` in metadata = milliseconds since Unix epoch (machine-readable)
- `datetime` in query = human-readable format (ISO 8601)
- They represent the **same moment in time**, just different formats

```
1682419200000 ms ÷ 1000 = 1682419200 seconds
1682419200 seconds since Jan 1, 1970 = 2023-04-24 10:00:00 UTC
```

**How It Works:**
1. Find snapshot with closest timestamp → snapshot ID 8945366420315465341
2. Open manifest list: `snap-8945366420315465341-1-61e14d08.avro`
3. Get manifest entry files
4. Read only those Parquet files
5. See data exactly as it was at that timestamp

**Old data is never deleted** - it's still in Parquet files, just not referenced by current snapshot.

---

### Concurrent Readers Without Locks

Without snapshots (Hadoop/Hive):
```
Writer: Creating new files, deleting old files
Reader 1: Mid-read of file writer is deleting → ERROR
Reader 2: Which files should I read? → INCONSISTENCY
```

With Iceberg snapshots:
```
current-snapshot-id = 9056477531426576452

Writer: Appends new data, creates new snapshot ID 9056477531426576453
Reader 1: Still reading snapshot 9056477531426576452 → CONSISTENT
Reader 2: Still reading snapshot 9056477531426576452 → CONSISTENT  
Reader 3: Reading new snapshot 9056477531426576453 → LATEST
```

**Why This Works:**
- Snapshots are immutable
- Manifest lists are immutable
- Manifest entry files are immutable
- Old Parquet files never deleted
- Readers grab snapshot ID at start, read that version forever

---

### Schema Evolution Without Rewrites

Old way (Hadoop/Hive):
```
Schema: [order_id, customer_id, order_date, total_amount]
Add new column: [order_id, customer_id, order_date, total_amount, discount_amount]
Problem: Need to rewrite all existing Parquet files!
```

Iceberg way:
```
schema: {
  "1": "order_id",
  "2": "customer_id",
  "3": "order_date",
  "4": "total_amount",
  "5": "discount_amount"  ← NEW
}

Old Parquet files have schema: [1, 2, 3, 4]
New Parquet files have schema: [1, 2, 3, 4, 5]

Query engine:
  - Reading old file? Get 1,2,3,4; discount_amount = null
  - Reading new file? Get 1,2,3,4,5

No rewrite needed!
```

**This is possible because:**
- Field IDs are immutable (not column positions)
- Metadata tracks which columns exist in which files
- Query engine handles missing columns

---

### Predicate Pushdown & File Pruning

Manifest entry files store statistics per column:

```json
{
  "file_path": "/app/datawarehouse/dw_orders/data/00000-1-abc123.parquet",
  "metrics": {
    "4": {  # total_amount column
      "lower_bound": 100.00,
      "upper_bound": 50000.00
    }
  }
}
```

**Query:** `WHERE total_amount > 45000`

**Query Planner:**
- File 1: total_amount [100, 50000] → Keep (45000 < 50000, might match)
- File 2: total_amount [1000, 30000] → Skip (max 30000 < 45000, no match)
- File 3: total_amount [45100, 49999] → Keep (definitely matches)

**Result:** Skip files without reading them = massive performance gain

---

## 📊 Metadata File Hierarchy Visualization

When you run the ETL, this hierarchy is created:

```
datawarehouse/
└── dw_orders/
    ├── data/
    │   ├── 00000-1-d3fb5e49.parquet  ← Snapshot 1
    │   ├── 00001-1-e4fc5f50.parquet  ← Snapshot 1
    │   ├── 00002-1-f5gd6g51.parquet  ← Snapshot 2
    │   └── 00003-1-g6he7h52.parquet  ← Snapshot 2
    │
    └── metadata/
        ├── v1.metadata.json (ROOT)
        │   ├── current-snapshot-id: 9056477531426576452
        │   └── snapshots[].manifest-list: snap-*.avro
        │
        ├── snap-9056477531426576452-1-72f25b2a.avro (Current)
        │   └── references: 72f25b2a-0edf-4a81-9bf3-d66d36108d5a-m0.avro
        │
        ├── 72f25b2a-0edf-4a81-9bf3-d66d36108d5a-m0.avro (File List)
        │   ├── 00000-1-d3fb5e49.parquet (status: EXISTING)
        │   ├── 00001-1-e4fc5f50.parquet (status: EXISTING)
        │   ├── 00002-1-f5gd6g51.parquet (status: ADDED)
        │   └── 00003-1-g6he7h52.parquet (status: ADDED)
        │
        ├── snap-8945366420315465341-1-61e14d08.avro (Previous)
        │   └── references: 61e14d08-1234-5678-9012-m0.avro
        │
        ├── 61e14d08-1234-5678-9012-m0.avro (Historical File List)
        │   ├── 00000-1-d3fb5e49.parquet (status: ADDED)
        │   └── 00001-1-e4fc5f50.parquet (status: ADDED)
        │
        ├── version-hint.text: "v1.metadata.json"
        ├── .v1.metadata.json.crc
        ├── .snap-9056477531426576452-1-72f25b2a.avro.crc
        └── .72f25b2a-0edf-4a81-9bf3-d66d36108d5a-m0.avro.crc
```

---

## 🔄 ETL to Metadata Generation

When the ETL calls `spark_df.writeTo(f"dw_{entity}").createOrReplace()`:

1. **Spark converts DataFrame to Parquet** → writes to `data/` directory
2. **Iceberg creates manifest entry file** (*-m0.avro) → lists Parquet files + statistics
3. **Iceberg creates manifest list** (snap-*.avro) → points to manifest entry
4. **Iceberg updates root metadata** (v1.metadata.json) → adds new snapshot
5. **Iceberg updates version hint** (version-hint.text) → points to new metadata version
6. **CRC checksums generated** → validates integrity

All of this happens **automatically** - that's the Iceberg magic!

---

## 🔒 ACID Compliance in Apache Iceberg

ACID is the foundation of reliable databases. Iceberg achieves full ACID compliance through its metadata-based architecture - without traditional locking mechanisms. Let's understand each component:

### What is ACID?

| Property | Meaning | Traditional Approach |
|----------|---------|----------------------|
| **Atomicity** | All-or-nothing transactions | Rollback logs |
| **Consistency** | Data integrity maintained | Foreign keys, constraints |
| **Isolation** | Concurrent ops don't interfere | Locking & blocking |
| **Durability** | Changes persist after commit | Write-ahead logs |

### How Iceberg Achieves Atomicity

**Atomicity = All or nothing. Either entire write succeeds or fails completely.**

#### Traditional Database Approach:
```
Transaction Log:
├─ Write file 1 ✓
├─ Write file 2 ✓
├─ Update table metadata ✓
└─ Commit

If step 3 fails: Rollback all changes
```

**Problem:** Complex, slow, requires transaction logs and rollbacks

#### Iceberg Approach:
```
Step 1: Write new Parquet files to /data
        (If fails, no harm - old snapshot still valid)

Step 2: Create new manifest files
        (Immutable, no locks needed)

Step 3: Update v1.metadata.json
        (ATOMIC - single JSON file update)
        OLD: {"current-snapshot-id": 100, ...}
        NEW: {"current-snapshot-id": 101, ...}

Result: Either old snapshot or new snapshot is visible, NEVER partial state!
```

**Why This Works:**
- Data files are immutable (append-only)
- Manifest files are immutable
- Only `v1.metadata.json` changes
- Filesystem provides atomic file replacement
- **No rollback needed** - old snapshot is always valid fallback

**Example Scenario:**
```
Writer starts:
- Creates 2 new Parquet files (temp location)
- Creates new manifest files

Halfway through v1.metadata.json update:
- Filesystem crash!
- Old v1.metadata.json still intact
- Table still pointing to Snapshot 100
- New files ignored
- Zero corruption!

vs. Traditional DB:
- Partially written transaction
- Inconsistent state
- Recovery needed
```

---

### How Iceberg Achieves Consistency

**Consistency = Table schema and data integrity never violated.**

#### At the Metadata Level:
```
v1.metadata.json enforces:
{
  "schema": {
    "1": {"name": "order_id", "type": "string", "required": true},
    "4": {"name": "total_amount", "type": "decimal(10,2)"}
  }
}

Every Parquet file written MUST follow this schema.
Query engine validates data types, not nullability, etc.

New files with missing column? Field IDs handle it.
New files with extra column? Query engine ignores unknown IDs.
```

#### At the Data Level:
```
Manifest files track statistics:
{
  "file": "data/00000-1-abc.parquet",
  "metrics": {
    "4": {
      "lower_bound": 100.00,
      "upper_bound": 50000.00,
      "nan_count": 0,
      "null_count": 15,
      "distinct_count": 1234
    }
  }
}

Query planner uses stats for validation:
- Column 4 should be decimal ✓
- Values in range [100, 50000] ✓
- 15 nulls acceptable (schema allows) ✓
```

#### Schema Evolution Without Breaking:
```
V1 Schema (May 2023): order_id, customer_id, order_date, total_amount
V2 Schema (June 2023): order_id, customer_id, order_date, total_amount, discount

Old files have: [1, 2, 3, 4]
New files have: [1, 2, 3, 4, 5]

Consistency = Query engine handles missing column 5 in old files
             Always returns consistent schema to user
```

---

### How Iceberg Achieves Isolation

**Isolation = Concurrent readers/writers don't see partial updates. Each sees a consistent snapshot.**

#### The Problem with Hadoop/Hive:
```
10:00 AM: Reader A starts query
         "SELECT COUNT(*) FROM orders"
         Finds files: A, B, C, D

10:01 AM: Writer writes new files
         "Creates file E, deletes file D"
         (Directory listing changes)

10:02 AM: Reader A continues
         "Includes file E but file D is gone"
         Query fails or returns wrong count!

10:03 AM: Reader B starts query
         "SELECT COUNT(*) FROM orders"
         Sees files: A, B, C, E
         Different count than Reader A!
```

#### Iceberg's Snapshot Isolation:
```
10:00 AM: Reader A opens table
         snapshot-id: 100
         Reads v1.metadata.json → Locks onto Snapshot 100
         "My snapshot list: [file-A, file-B, file-C, file-D]"

10:01 AM: Writer writes new data
         Creates new files
         Updates v1.metadata.json
         current-snapshot-id: 101

10:02 AM: Reader A continues
         Still reading Snapshot 100
         Still sees: [file-A, file-B, file-C, file-D]
         Unaffected! ✓

10:03 AM: Reader B opens table
         snapshot-id: 101 (current)
         Reads updated v1.metadata.json
         Sees different file list but fully consistent

Result: NO CONFLICTS, NO LOCKING!
```

#### Why This Works:
```
Snapshot = Immutable view of table
                        ↓
        v1.metadata.json → snapshots array
                        ↓
        [snapshot-100, snapshot-101, snapshot-102]
                        ↓
        Each snapshot immutable → Never changes
                        ↓
        Reader picks snapshot at start → Uses that forever
                        ↓
        Writer creates new snapshot → Doesn't affect old readers
                        ↓
        RESULT: Full isolation without locks!
```

#### Concurrent Writers?
```
Writer A: Creates new Parquet files → Calls writeTo(table)
Writer B: Creates new Parquet files → Calls writeTo(table)

Without atomicity: Race condition!
With Iceberg: 

- Writer A updates v1.metadata.json first
  current-snapshot-id: 102
  snapshots[].manifest-list: snap-...-A.avro

- Writer B tries to update v1.metadata.json
  Detects v1.metadata.json changed (old vs new)
  Retry: Read latest v1.metadata.json
  Merge: Combine snapshot 102 (from A) + new snapshot 103 (from B)
  Write: New v1.metadata.json with both snapshots
  current-snapshot-id: 103

Result: Both writes succeed without conflicts!
```

---

### How Iceberg Achieves Durability

**Durability = Once committed, changes persist even after crash.**

#### Iceberg's Approach:
```
1. Write Parquet files to persistent storage (/data)
   → Even if crash, files survive
   
2. Write manifest files to persistent storage (/metadata)
   → Immutable, durable
   
3. Update v1.metadata.json in persistent storage
   → Atomic write to filesystem
   → Either old or new version exists
   
4. All writes to replicated filesystem (HDFS, S3, etc)
   → Multiple copies ensure durability

Result: No data loss possible!
```

#### Verification:
```
After crash, check table state:
1. Open version-hint.text → "v1.metadata.json"
2. Open v1.metadata.json → Read current-snapshot-id
3. If v1.metadata.json is partial/corrupt?
   → Filesystem corruption (rare)
   → Fall back to backup snapshots
   → Or restore from previous snapshot

CRC checksums detect corruption:
{
  v1.metadata.json ← Read OK
  .v1.metadata.json.crc ← Checksum validates it
}
```

---

### ACID vs Traditional Databases

#### Traditional Database (PostgreSQL, MySQL):
```
ACID through:
- Atomicity: Transaction logs, rollback
- Consistency: Foreign keys, constraints
- Isolation: Locks on rows/tables, wait queues
- Durability: Write-ahead logs to disk

Problem:
  SELECT COUNT(*) FROM orders  (Lock entire table for write)
  Blocks concurrent writes!
  Performance suffers with scale
```

#### Iceberg:
```
ACID through:
- Atomicity: Immutable files + atomic metadata swap
- Consistency: Field IDs + schema validation
- Isolation: Snapshot versioning (no locks!)
- Durability: Replicated filesystem

Benefit:
  Multiple writers can write simultaneously
  Multiple readers can read simultaneously
  Zero locks!
```

---

### Real-World Example: ACID in Action

**Scenario:** Online store with concurrent operations

```
10:00:00 - Reader A (Analyst):
          SELECT SUM(total_amount) FROM orders
          Locks onto Snapshot 100 (1000 orders, $50,000)

10:00:05 - Writer B (Backend):
          Write 100 new orders from today
          Creates new Parquet files
          Updates to Snapshot 101 (1100 orders, $55,000)

10:00:10 - Reader A (Still Running):
          Continues reading Snapshot 100
          Gets final sum: $50,000
          ✓ Correct for the point-in-time!

10:00:15 - Reader C (Dashboard):
          SELECT SUM(total_amount) FROM orders
          Locks onto Snapshot 101 (current)
          Gets sum: $55,000
          ✓ Sees latest data!

10:00:20 - Writer D (Backend):
          Tries to write 50 more orders
          Detects Snapshot 101 exists
          Merges with previous snapshot
          Creates Snapshot 102 (1150 orders, $57,500)

10:00:30 - Reader A (Finally Done):
          Thanks Snapshot 100!
          No lock conflicts, no waiting
          A, B, C, D all completed successfully
          
Result: Full ACID + No locks + Optimal concurrency!
```

---

## ❄️ Iceberg vs Hadoop/Hive

| Aspect | Hadoop/Hive | Iceberg |
|--------|-------------|---------|
| **Time-Travel** | Not possible | Query any historical snapshot |
| **ACID Transactions** | Not reliable | Atomic metadata swaps |
| **Concurrent Readers** | Issues during writes | Full isolation via snapshots |
| **Schema Evolution** | Difficult, rewrites data | Field IDs enable seamless evolution |
| **Partition Evolution** | Not supported | Dynamic partitioning |
| **File Statistics** | External metastore | Embedded in metadata |
| **Data Deletion** | Slow, complex | Tracked in manifest status |

---

## 🔍 Files to Examine

To truly understand Iceberg, examine these files in order:

1. **`version-hint.text`** (2 bytes) - Where to start
2. **`v1.metadata.json`** (readable JSON) - Complete table definition
3. **`snap-*.avro`** (Avro binary) - Snapshot manifests
4. **`*-m0.avro`** (Avro binary) - File listings
5. **`*.parquet`** (Parquet binary) - Actual data

**To inspect Avro files:**
```python
import fastavro
with open('snap-*.avro', 'rb') as f:
    reader = fastavro.reader(f)
    for record in reader:
        print(record)
```

---

## ❓ Q&A: Understanding Iceberg Concepts

### Q1. What's a Snapshot?

**A Snapshot is a point-in-time version of your entire table.**

A snapshot captures the exact state of the table at a specific moment - which files exist, how many rows, what schema, etc. Every time data is written to an Iceberg table, a **new snapshot is created**.

**Key Points:**
- **Immutable** - Once created, a snapshot never changes
- **Timestamped** - Each snapshot has a creation timestamp
- **Identified by ID** - Unique snapshot ID (e.g., 9056477531426576452)
- **References manifest list** - Points to which files belong to this snapshot
- **Never deletes old data** - Old snapshots still reference old files

**Example:**
```
Day 1: Write 1000 rows → Snapshot ID: 9056477531426576452 (timestamp: 2026-04-25 10:00:00)
Day 2: Write 500 rows → Snapshot ID: 8945366420315465341 (timestamp: 2026-04-26 10:00:00)
Day 3: Write 300 rows → Snapshot ID: 7834255309204354230 (timestamp: 2026-04-27 10:00:00)

Today (Day 5):
- current-snapshot-id = 7834255309204354230 (latest, 1800 total rows)
- Can query snapshot from Day 2: 1500 total rows
- Can query snapshot from Day 1: 1000 total rows
- All data is preserved!
```

**Why Snapshots Matter:**
- Enable time-travel queries (query data from any point in time)
- Enable concurrent reads without locks (each reader picks a snapshot)
- Enable rollback (revert to previous snapshot if needed)
- Track complete history of table changes

---

### Q2. What's a Manifest List and Manifest File?

**These are the "glue" between snapshots and actual data files.**

#### Manifest List (snap-*.avro)
- **Purpose**: Points to which manifest files belong to a snapshot
- **One per snapshot**: Each snapshot has exactly one manifest list
- **Contains**: File paths, row counts, partition info
- **Format**: Avro (binary)
- **Example filename**: `snap-9056477531426576452-1-72f25b2a.avro`

**What does a manifest list do?**
```
Snapshot: 9056477531426576452
    ↓
Manifest List: snap-9056477531426576452-1-72f25b2a.avro
    ↓
    Says: "This snapshot has 2 manifest files:
           - 72f25b2a-0edf-4a81-9bf3-d66d36108d5a-m0.avro (added 1500 rows)
           - 61e14d08-1234-5678-9012-3456789abcde-m0.avro (added 300 rows)"
```

#### Manifest File (*-m0.avro)
- **Purpose**: Lists all data files (Parquets) and their statistics
- **Multiple per snapshot**: One snapshot can have many manifest files
- **Contains**: File paths, record counts, column min/max values, partition values
- **Format**: Avro (binary)
- **Example filename**: `72f25b2a-0edf-4a81-9bf3-d66d36108d5a-m0.avro`

**What does a manifest file contain?**
```
Manifest File: 72f25b2a-0edf-4a81-9bf3-d66d36108d5a-m0.avro
    ↓
    Contains list of files:
    [
      {
        file_path: "data/00000-1-abc123.parquet",
        record_count: 750,
        metrics: { 
          order_id: [min: "O001", max: "O750"],
          total_amount: [min: 100.00, max: 50000.00]
        }
      },
      {
        file_path: "data/00001-1-def456.parquet",
        record_count: 750,
        metrics: { ... }
      }
    ]
```

**The Flow:**
```
v1.metadata.json (root)
    └─ current-snapshot-id: 9056477531426576452
    └─ snapshots[].manifest-list: snap-9056477531426576452-1-72f25b2a.avro
            │
            └─ Manifest List (snap-*.avro)
                    │
                    └─ references: [
                         72f25b2a-0edf-4a81-9bf3-d66d36108d5a-m0.avro,
                         61e14d08-1234-5678-9012-3456789abcde-m0.avro
                       ]
                            │
                            ├─ Manifest File 1 (*-m0.avro)
                            │      └─ data/00000-1-abc123.parquet
                            │      └─ data/00001-1-def456.parquet
                            │
                            └─ Manifest File 2 (*-m0.avro)
                                   └─ data/00002-1-ghi789.parquet
                                   └─ data/00003-1-jkl012.parquet
```

---

### Q3. Can There Be Multiple Snapshots, Manifest Lists, and Manifest Files?

**Short Answer: YES to all three! Here's why you might have many:**

#### Multiple Snapshots

**Yes, always!** Every time you write data, a new snapshot is created.

```
Day 1: ETL run → Snapshot 1 (9056477531426576452)
Day 2: ETL run → Snapshot 2 (8945366420315465341)
Day 3: ETL run → Snapshot 3 (7834255309204354230)
...
Day 100: ETL run → Snapshot 100

Your v1.metadata.json contains ALL 100 snapshots in history!
```

**Why keep all snapshots?**
- Enable time-travel (query any historical version)
- Track complete change history
- Enable rollback if corruption detected
- Support auditing and compliance

#### Multiple Manifest Lists

**One manifest list per snapshot, so if you have 100 snapshots, you have 100 manifest lists!**

```
Snapshot 1 → snap-9056477531426576452-1-72f25b2a.avro
Snapshot 2 → snap-8945366420315465341-1-61e14d08.avro
Snapshot 3 → snap-7834255309204354230-1-50d03c97.avro
...
Snapshot 100 → snap-1234567890123456789-1-xyz99999.avro
```

Each is immutable and points to its own set of manifest files.

#### Multiple Manifest Files

**Yes! One snapshot can have multiple manifest files.**

When does this happen?
- **Large tables**: Split manifest entries across files (e.g., 1000 files per manifest)
- **Multi-partition writes**: Different partitions might get different manifest files
- **Compaction**: Combine old files into new manifest entries

**Example:**
```
Snapshot: 9056477531426576452
    └─ Manifest List references 3 manifest files:
           ├─ manifest-file-1.avro (files 00000-00099)
           ├─ manifest-file-2.avro (files 00100-00199)
           └─ manifest-file-3.avro (files 00200-00299)
    
Total: 300 Parquet files tracked across 3 manifest files
```

**How many files can be in one table?**
```
Example with partitioned data:

Date: 2026-04-25 → 50 Parquet files → 1 manifest file
Date: 2026-04-26 → 50 Parquet files → 1 manifest file
Date: 2026-04-27 → 50 Parquet files → 1 manifest file

Total in snapshot: 150 Parquet files, 3 manifest files, 1 manifest list
```

---

### Q4. Difference Between Manifest List and Manifest File

**This is the key distinction:**

| Aspect | Manifest List | Manifest File |
|--------|---------------|---------------|
| **Purpose** | Links snapshot to manifest files | Lists actual data files (Parquets) |
| **Level** | One level above manifest files | Points to Parquet files |
| **Quantity** | 1 per snapshot | Multiple per snapshot possible |
| **File Size** | Small (KB) | Medium (KB-MB) |
| **Naming** | `snap-[snapshot-id]-1-[uuid].avro` | `[uuid]-m0.avro` |
| **Contains** | Manifest file references | Parquet file references + statistics |
| **Mutability** | Immutable after creation | Immutable after creation |
| **Update Frequency** | Created once per write | Created/referenced per write |
| **Read During Query** | First, to find manifest files | Second, to find Parquet files |

---

### Q5. What Happens When You Write New Data?

**A cascading metadata update happens:**

```
1. ETL writes data → 100 new rows in Parquet format
        ↓
2. Data written to: data/00100-1-new123.parquet
        ↓
3. Iceberg creates Manifest File entry:
        {
          file_path: "data/00100-1-new123.parquet",
          record_count: 100,
          metrics: { ... }
        }
        ↓
4. Iceberg creates/updates Manifest List:
        "This snapshot now has 2 manifest files instead of 1"
        ↓
5. Iceberg updates v1.metadata.json:
        {
          current-snapshot-id: NEW_SNAPSHOT_ID,
          snapshots: [ ...all old + new snapshot... ]
        }
        ↓
6. Iceberg updates version-hint.text:
        "v1.metadata.json"  (stays same, but JSON content changed)
        ↓
7. CRC checksums generated for validation
        ↓
RESULT: Old snapshot still valid, new snapshot created, concurrent readers unaffected!
```

---

### Q6. How Does Time-Travel Actually Work?

**Three-step process:**

**Scenario:** You want to see table state from April 24 at 10:00 AM

```
Step 1: Find Matching Snapshot
v1.metadata.json snapshots array:
[
  { snapshot-id: 7834255309204354230, timestamp-ms: 1682419200000 },  ← April 25, 10:00 AM
  { snapshot-id: 8945366420315465341, timestamp-ms: 1682332800000 },  ← April 24, 10:00 AM ✓ MATCH
  { snapshot-id: 9056477531426576452, timestamp-ms: 1682246400000 }   ← April 23, 10:00 AM
]

Query: SELECT * FOR SYSTEM_TIME AS OF '2023-04-24 10:00:00'
→ Select snapshot 8945366420315465341

Step 2: Load Manifest List
Manifest List: snap-8945366420315465341-1-61e14d08.avro
→ Lists which manifest files existed at that time
→ Only manifest files from April 24 or earlier

Step 3: Load Manifest Files & Parquets
Manifest Files referenced from April 24 snapshot
→ Get list of Parquet files
→ Read only those files
→ Return data exactly as it was on April 24!
```

**Key Insight:** Parquet files from April 25 & 26 are completely ignored. You only read what existed on April 24!

---

### Q7. What's the Purpose of Field IDs in Schema?

**Field IDs enable schema evolution without rewriting data.**

```
Original Schema (April 2026):
{
  "1": "order_id",
  "2": "customer_id",
  "3": "order_date",
  "4": "total_amount"
}

New Schema (May 2026):
{
  "1": "order_id",
  "2": "customer_id",
  "3": "order_date",
  "4": "total_amount",
  "5": "discount_amount"  ← NEW COLUMN
}

Old Parquet Files (created in April):
- Have columns: 1, 2, 3, 4
- Don't have column: 5

New Parquet Files (created in May):
- Have columns: 1, 2, 3, 4, 5

Query Execution:
When reading mixed files:
- Old file? Return 1,2,3,4; column 5 = NULL
- New file? Return 1,2,3,4,5
- User sees: Complete table with consistent schema!

Why Field IDs Work:
- IDs are IMMUTABLE (never change)
- Columns identified by ID, not position
- If you rename column 4 "amount" → "price":
  Old files still have ID 4, renamed in schema
  Query engine finds ID 4 and returns "price" column
  Everything works seamlessly!
```

---

### Q8. Why Are Manifest Files in Avro Format?

**Three reasons:**

1. **Efficient Binary Format** - Smaller than JSON, faster to parse
2. **Serializable Schema** - Avro schema travels with data
3. **Evolution Support** - Can add fields to Avro without breaking readers

**Iceberg could use JSON for manifest files, but Avro is faster:**

```
JSON Manifest List: 5 KB
Avro Manifest List: 2 KB (60% smaller)

For a table with 10,000 snapshots:
JSON: 50 MB of metadata
Avro: 20 MB of metadata

When loading all snapshots, this matters!
```

---

### Q9. Can Multiple Readers Query the Same Table Simultaneously?

**Yes! With complete isolation. Here's how:**

```
Timeline:

10:00 AM: Reader A opens table
         → Reads version-hint.text → "v1.metadata.json"
         → Reads v1.metadata.json → current-snapshot-id: 100
         → Locks onto Snapshot 100
         → Reader A will only see data from Snapshot 100

10:01 AM: Reader B opens table
         → Reads version-hint.text → "v1.metadata.json"  (same file)
         → Reads v1.metadata.json → current-snapshot-id: 100  (same)
         → Also locks onto Snapshot 100
         → Reader B also sees Snapshot 100

10:02 AM: Writer writes new data
         → Creates new Parquet files
         → Updates manifest files
         → Creates NEW Snapshot 101
         → Updates v1.metadata.json
         → current-snapshot-id: 101

10:03 AM: Reader C opens table
         → Reads version-hint.text → "v1.metadata.json"
         → Reads v1.metadata.json → current-snapshot-id: 101
         → Locks onto Snapshot 101
         → Reader C sees NEW data from Snapshot 101

10:04 AM: Status
         Reader A: Still reading Snapshot 100 (not affected by write!)
         Reader B: Still reading Snapshot 100 (not affected by write!)
         Reader C: Reading Snapshot 101 (sees new data)
         Writer: Can write more data without affecting readers!
```

**Why This Works:**
- Snapshots are immutable
- Manifest lists are immutable
- Old Parquet files never deleted
- Readers grab snapshot ID and stick with it
- No locks needed!

---

### Q10. What Happens If You Delete Files?

**Iceberg tracks file deletion in manifest entries:**

```
Manifest File Status Codes:
1 = ADDED    (new file in this snapshot)
2 = EXISTING (file from previous snapshot, still included)
3 = DELETED  (file was in previous snapshot, now removed)

Example:
Snapshot 1 - File A (status: ADDED)
Snapshot 2 - File A (status: EXISTING), File B (status: ADDED)
Snapshot 3 - File A (status: DELETED), File B (status: EXISTING)

At Snapshot 3:
- Only File B is included (File A marked as DELETED)
- File A can be garbage collected if no snapshot references it
- Query from Snapshot 1 or 2 still sees File A
- Query from Snapshot 3 doesn't see File A
```

**Garbage Collection:**
```
Old Snapshots: [Snapshot 1, Snapshot 2, Snapshot 3]
File A referenced by: [Snapshot 1, Snapshot 2]
File B referenced by: [Snapshot 2, Snapshot 3]

Can delete File A? Only if all referencing snapshots are deleted
Can delete File B? No, Snapshot 3 still references it

If you expire Snapshot 1 and 2:
- Now only Snapshot 3 exists
- File A can be deleted (no snapshot references it)
- File B still needed (Snapshot 3 references it)
```

---

### Q11. How Is ACID Achieved Without Locks?

**Through atomic metadata swaps:**

```
Traditional Database (Locking):
├─ Writer takes lock
├─ Writer modifies table
├─ Readers blocked (waiting for lock)
├─ Writer releases lock
└─ Readers proceed

Iceberg (No Locks):
├─ Writer creates new Parquet files (in temp location)
├─ Writer creates new manifest files (immutable)
├─ Writer creates new manifest list (immutable)
├─ Writer atomically updates v1.metadata.json JSON
│  (This is the ONLY mutable operation)
├─ Concurrent readers unaffected (reading old snapshot)
└─ Readers starting now see new snapshot
```

**Why v1.metadata.json Update is Atomic:**
- JSON file is small (KBs)
- Filesystem provides atomic file replacement
- Old readers don't care (snapshot ID already loaded)
- New readers see new current-snapshot-id

**Result: ACID without locks!**

---

## 📚 Key Takeaways

1. **Data & Metadata Separation** - Data is immutable, metadata is the mutable interface
2. **Snapshots Enable Time-Travel** - Each write creates a snapshot; old data never deleted
3. **Atomic Metadata Updates** - ACID transactions via atomic JSON swaps
4. **Statistics Enable Optimization** - Manifest entries store file-level statistics for pruning
5. **Schema Evolution Without Rewrites** - Field IDs allow seamless schema changes
6. **Concurrent Isolation** - Readers see consistent snapshots without locks
7. **Manifest Hierarchy** - Snapshots → Manifest Lists → Manifest Files → Parquets
8. **File Tracking** - Status codes track file lifecycle (ADDED, EXISTING, DELETED)

---

**Project Created:** April 2026  
**Purpose:** Understanding Apache Iceberg metadata system from first principles