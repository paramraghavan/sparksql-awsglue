# Stage-Aware Advice: Better Recommendations

## The Problem with Aggregated Advice

**Old behavior:**
```
❌ Large shuffle — 100GB moved across network
   Your cell moved 100GB of data
   (But which stage? Is it one huge shuffle or 3 smaller ones?)
```

**New behavior:**
```
✅ Large shuffle in Stage 1 — join or groupBy moved a lot of data
   Spark moved 50GB of data across the network in Stage 1
   (Now you know exactly which stage to optimize!)
```

---

## What Changed

### 1. Per-Stage Problem Detection

Added function `_find_problem_stages()` that identifies:
- **Which stage has the largest shuffle** (shuffleWriteBytes)
- **Which stage has the most disk spill** (diskBytesSpilled)
- **Which stages are underutilized** (too few tasks)

### 2. Advice Template Updates

All advice rules now use **stage-specific information**:

```python
# BEFORE:
title = "Large shuffle — join or groupBy moved a lot of data"
what  = "Spark moved {shuffle_write} of data across the network"

# AFTER:
title = "Large shuffle in {max_shuffle_stage} — join or groupBy moved a lot of data"
what  = "Spark moved {max_shuffle_bytes} of data across the network in {max_shuffle_stage}"
```

Updated rules:
- ✅ **Disk spill** — Shows which stage spilled
- ✅ **Large shuffle** (500MB-1GB) — Shows which stage, how much
- ✅ **Very large shuffle** (>1GB) — Shows which stage, how much

### 3. New Template Variables

Added to substitution dictionary:
- `max_shuffle_stage` — e.g., "Stage 2"
- `max_shuffle_bytes` — e.g., "50GB"
- `max_spill_stage` — e.g., "Stage 1"
- `max_spill_bytes` — e.g., "100GB"

---

## Example: Before vs After

### Scenario: Job with 100GB total shuffle

**Before:**
```
🟡 Large shuffle — 100GB moved across network
   Your cell moved 100GB of data across the network
   (No idea which stage caused it)
```

**After:**
```
🟡 Large shuffle in Stage 1 — join or groupBy moved a lot of data
   Spark moved 50GB of data across the network in Stage 1

🟡 Large shuffle in Stage 2 — join or groupBy moved a lot of data
   Spark moved 50GB of data across the network in Stage 2
   (Two separate bottlenecks, each 50GB!)
```

---

## How It Helps

### Case 1: Single Stage Problem (Easy to Fix)
```
Stage 0: 0GB shuffle
Stage 1: 80GB shuffle ← THIS ONE!
Stage 2: 20GB shuffle

Advice: "In Stage 1, try broadcast() or repartition on the join key"
→ User now knows exactly where to optimize
```

### Case 2: Multiple Stage Problems (Needs Different Solutions)
```
Stage 0: 0GB shuffle
Stage 1: 40GB shuffle (join operation)
Stage 2: 0GB shuffle
Stage 3: 60GB shuffle (groupBy operation)

Advice:
- "In Stage 1, try broadcast() for the join"
- "In Stage 3, increase partitions to reduce per-partition shuffle"
→ User knows each stage needs a different approach
```

### Case 3: Disk Spill
```
Before:
❌ Your data spilled to disk (100GB)
   (Which stage ran out of memory?)

After:
❌ Your data spilled to disk (Stage 2)
   Spark ran out of memory in Stage 2 and wrote 100GB to disk
   (User knows to focus optimization on Stage 2)
```

---

## Complete Report Output Example

```
⚡ sparkmonitor
🟡 Warnings found — review the advice cards

Full metrics:
├─ Total time: 150s
├─ Parallel tasks: 1200
├─ Network shuffle: 100GB ⚠
└─ Spilled to disk: None ✓

Per-stage breakdown:
Stage 0: 1000 tasks, 125s, 0GB shuffle → ✓
Stage 1: 150 tasks, 20s, 50GB shuffle → ⚠
Stage 2: 50 tasks, 5s, 50GB shuffle → ⚠

Advice cards:
🟡 Large shuffle in Stage 1 — join or groupBy moved a lot of data
   Spark moved 50GB of data in Stage 1.
   Try broadcast() if joining with a small table.

🟡 Large shuffle in Stage 2 — join or groupBy moved a lot of data
   Spark moved 50GB of data in Stage 2.
   Try repartition on the join key to pre-shuffle before joining.
```

---

## Summary: Question Answered

**User asked:** "Does the advice use per-stage breakdown to let user know?"

**Answer:**
- ❌ **Before:** No, advice was based only on aggregated totals
- ✅ **After:** Yes, advice now identifies which specific stage has the problem

**Benefits:**
1. **Clarity** — User knows exactly which stage to optimize
2. **Targeted fixes** — Different stages may need different solutions
3. **Accurate metrics** — Shows per-stage amounts, not just totals
4. **Better debugging** — "It's Stage 3 with 50GB shuffle" is more actionable than "100GB total"

Now when you run `%%measure`, the advice cards will tell you **which stage** has issues and **how much** work is in that specific stage. 🎯
