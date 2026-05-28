# 🗺️ PySpark Handbook: Quick Navigation Map

> **Quick reference to find exactly what you need, when you need it.**

---

## 🎯 I Want To...

### Learn Spark Basics
- **Never used Spark before** → [Quick Start (30 min)](01-fundamentals/00-quick-start.md)
- **Understand how Spark works** → [Architecture Deep Dive (1 hour)](01-fundamentals/01-spark-architecture.md)
- **Learn DataFrame operations** → [DataFrame Basics (1.5 hours)](01-fundamentals/02-dataframes-basics.md)
- **Write SQL in Spark** → [Spark SQL Guide (1 hour)](01-fundamentals/04-sparksql-basics.md)

### Master Real-World Patterns
- **Learn 20 essential patterns** → [Real-World Patterns (2 hours)](02-data-operations/00-real-world-patterns.md)
  - Deduplication, Top N, Running Total, Joins, Pivots, Explodes...
- **Window functions mastery** → [Section 2 of Patterns](02-data-operations/00-real-world-patterns.md)
- **Complex joins guide** → [Pattern 8](02-data-operations/00-real-world-patterns.md)

### Pass a Job Interview
- **Full interview prep guide** → [Interview Overview](08-interview-preparation/README.md)
- **50+ core concept Q&A** → [Core Concepts Q&A](08-interview-preparation/01-core-concepts-qa.md)
- **40+ DataFrame Q&A** → [DataFrame/SQL Q&A](08-interview-preparation/02-dataframes-sql-qa.md)
- **35+ performance Q&A** → [Performance Q&A](08-interview-preparation/03-performance-qa.md)
- **40+ joins/partition Q&A** → [Joins & Partitions Q&A](08-interview-preparation/04-joins-partitions-qa.md)
- **30+ troubleshooting Q&A** → [Troubleshooting Q&A](08-interview-preparation/05-troubleshooting-qa.md)
- **Solve 15 coding problems** → [Coding Patterns](08-interview-preparation/06-coding-patterns.md)
- **Design an ETL pipeline** → [System Design](08-interview-preparation/07-system-design.md)
- **Answer behavioral questions** → [Behavioral Scenarios](08-interview-preparation/08-behavioral-scenarios.md)
- **Quick pre-interview review** → [Cheatsheet (print this!)](09-reference/cheatsheet.md)

### Build Production Systems
- **Learn production patterns** → [Production Patterns](06-production-patterns/README.md)
  - Config-driven ETL, data comparison, reusable libraries
- **Optimize performance** → [Performance Optimization](04-performance-optimization/README.md)
  - Caching strategies, tuning, Catalyst, Tungsten
- **Integrate with AWS** → [AWS Integration](05-aws-integration/)
  - EMR, Glue, S3, partitioning strategies

### Fix a Slow/Broken Job
- **Job stuck at 99%** → [Data Skew Problems (20 min)](07-troubleshooting/03-skew-problems.md)
- **Out of memory error** → [Memory Errors (20 min)](07-troubleshooting/02-memory-errors.md)
- **Job never starts/takes forever** → [Job Stuck Issues (20 min)](07-troubleshooting/01-job-stuck-issues.md)
- **Shuffle is failing** → [Shuffle Failures (20 min)](07-troubleshooting/04-shuffle-failures.md)
- **Job slowly gets slower over time** → [Performance Degradation (30 min)](07-troubleshooting/05-performance-degradation.md)
- **Config keeps failing** → [Configuration Errors (20 min)](07-troubleshooting/06-configuration-errors.md)
- **Schema/data type issues** → [Schema Issues (20 min)](07-troubleshooting/07-schema-issues.md)
- **AWS-specific errors** → [AWS Errors (20 min)](07-troubleshooting/08-aws-specific-errors.md)

### Look Up Quick Reference
- **Code snippets for any operation** → [Cheatsheet (print this!)](09-reference/cheatsheet.md)
- **Understand a term** → [Glossary](09-reference/glossary.md)
- **Configuration parameters** → [Configuration Reference](09-reference/configuration-reference.md)
- **Coming from Excel/SQL?** → [Excel Primer](10-excel-primer/excel_primer_data_engineer.md)

---

## 🎓 By Time Available

### I Have 30 Minutes
```
1. Read: 00-START-HERE.md (10 min)
2. Read: 01-fundamentals/00-quick-start.md (20 min)
```
→ Understand what Spark is

### I Have 2 Hours
```
1. Read: 01-fundamentals/01-spark-architecture.md (60 min)
2. Code: Run first program from 00-quick-start.md (30 min)
3. Skim: 09-reference/cheatsheet.md (30 min)
```
→ Understand Spark deeply, hands-on code

### I Have 8 Hours (Full Day)
```
1. Fundamentals: 01-spark-architecture.md (1 hour)
2. Quick Start: 01-fundamentals/00-quick-start.md (1 hour)
3. Patterns: 02-data-operations/00-real-world-patterns.md (2 hours)
4. Practice: Exercises (3 hours)
5. Reference: Cheatsheet review (1 hour)
```
→ Ready for productive work

### I Have 1 Week
**Option A: Interview Prep**
- Day 1: Core Concepts Q&A
- Day 2: DataFrame Q&A
- Day 3: Performance Q&A
- Day 4: Joins Q&A
- Day 5: Coding problems
- Day 6: System design
- Day 7: Rest & review

**Option B: Complete Learning**
- Days 1-2: Fundamentals
- Days 3-4: Real-world patterns
- Days 5-6: Troubleshooting
- Day 7: Practice & review

### I Have 4 Weeks
- Week 1: All fundamentals
- Week 2: Patterns + Exercises
- Week 3: Performance + Production patterns
- Week 4: Troubleshooting + Interview prep

---

## 🔍 By Topic

### Core Concepts
- Spark Architecture → [Fundamentals](01-fundamentals/01-spark-architecture.md)
- Lazy Evaluation → [Architecture](01-fundamentals/01-spark-architecture.md#lazy-evaluation)
- DAG & Execution → [Architecture](01-fundamentals/01-spark-architecture.md#execution-model-dag-stages-tasks)
- Partitions & Shuffles → [Core Concepts Q&A](08-interview-preparation/01-core-concepts-qa.md)

### DataFrame & SQL
- Reading/Writing Data → [DataFrame Basics](01-fundamentals/02-dataframes-basics.md)
- Select, Filter, Join → [DataFrame Basics](01-fundamentals/02-dataframes-basics.md)
- Aggregations → [Real-World Patterns](02-data-operations/00-real-world-patterns.md) (Pattern 10)
- Window Functions → [Real-World Patterns](02-data-operations/00-real-world-patterns.md) (Patterns 2-4)
- Spark SQL → [Spark SQL Basics](01-fundamentals/04-sparksql-basics.md)

### Performance
- Caching Strategies → [Performance Optimization](04-performance-optimization/01-caching-strategies.md)
- Join Optimization → [Patterns](02-data-operations/00-real-world-patterns.md) (Patterns 8-9)
- Partitioning Guide → [Fair Usage Guidelines](../fair_usage_guidelines/partition_user_guide.md)
- Shuffle Tuning → [Performance Q&A](08-interview-preparation/03-performance-qa.md)
- Catalyst Optimizer → [Performance Q&A](08-interview-preparation/03-performance-qa.md)

### Joins
- Join Types → [Patterns](02-data-operations/00-real-world-patterns.md) (Pattern 8)
- Broadcast Joins → [Patterns](02-data-operations/00-real-world-patterns.md) (Pattern 8)
- Join Interview Q&A → [Joins Q&A](08-interview-preparation/04-joins-partitions-qa.md)
- Join Strategies Deep Dive → [Join Strategies](../join-strategies/)

### Data Skew
- Understanding Skew → [Skew Problems](07-troubleshooting/03-skew-problems.md)
- Salting Technique → [Skew Problems](07-troubleshooting/03-skew-problems.md)
- Interview Q&A → [Joins Q&A](08-interview-preparation/04-joins-partitions-qa.md)

### AWS & EMR
- EMR Setup → [EMR Guide](../emr-jupyter-notebook.md)
- Glue Integration → [AWS Integration](05-aws-integration/)
- Partitioning Strategy → [Fair Usage Guidelines](../fair_usage_guidelines/partition_user_guide.md)
- S3 Best Practices → [Fair Usage Guidelines](../fair_usage_guidelines/quick_reference_card.md)

### Real Problems
- Job Stuck → [Troubleshooting](07-troubleshooting/01-job-stuck-issues.md)
- Memory Issues → [Troubleshooting](07-troubleshooting/02-memory-errors.md)
- Slow Job → [Troubleshooting](07-troubleshooting/05-performance-degradation.md)
- Shuffle Failures → [Troubleshooting](07-troubleshooting/04-shuffle-failures.md)

---

## 👥 By Role

### Data Engineer
**Priority**: Fundamentals → Production Patterns → Troubleshooting → Performance

Suggested path:
1. [Fundamentals](01-fundamentals/) (1 week)
2. [Real-World Patterns](02-data-operations/00-real-world-patterns.md) (1 week)
3. [Production Patterns](06-production-patterns/) (1 week)
4. [Troubleshooting](07-troubleshooting/) (1 week)
5. [Performance](04-performance-optimization/) (ongoing reference)

### Data Scientist
**Priority**: Quick Start → Patterns → Performance → Interview Prep

Suggested path:
1. [Quick Start](01-fundamentals/00-quick-start.md) (1 day)
2. [Real-World Patterns](02-data-operations/00-real-world-patterns.md) (1 week)
3. [Performance](04-performance-optimization/) (1 week)
4. [Interview Q&A](08-interview-preparation/) (as needed)

### ML Engineer
**Priority**: Patterns → Performance → System Design

Suggested path:
1. [Quick Start](01-fundamentals/00-quick-start.md) (1 day)
2. [Real-World Patterns](02-data-operations/00-real-world-patterns.md) (1 week)
3. [Performance](04-performance-optimization/) (1 week)
4. [System Design](08-interview-preparation/07-system-design.md) (1 week)

### Analytics Engineer
**Priority**: SQL → Patterns → Partitioning

Suggested path:
1. [Spark SQL](01-fundamentals/04-sparksql-basics.md) (1 day)
2. [Real-World Patterns](02-data-operations/00-real-world-patterns.md) (1 week)
3. [Partitioning](../fair_usage_guidelines/partition_user_guide.md) (1 week)
4. [Troubleshooting](07-troubleshooting/) (as needed)

---

## ⚡ By Problem Type

### Performance Problems
1. **Job is slow** → [Performance Degradation](07-troubleshooting/05-performance-degradation.md)
2. **Job stuck at 99%** → [Skew Problems](07-troubleshooting/03-skew-problems.md)
3. **Shuffle is slow** → [Performance Q&A](08-interview-preparation/03-performance-qa.md)
4. **Out of memory** → [Memory Errors](07-troubleshooting/02-memory-errors.md)

### Execution Problems
1. **Job never starts** → [Job Stuck Issues](07-troubleshooting/01-job-stuck-issues.md)
2. **Shuffle fails** → [Shuffle Failures](07-troubleshooting/04-shuffle-failures.md)
3. **Config fails** → [Configuration Errors](07-troubleshooting/06-configuration-errors.md)
4. **Data type issues** → [Schema Issues](07-troubleshooting/07-schema-issues.md)

### Data Problems
1. **Duplicates in data** → [Pattern 1 (Dedup)](02-data-operations/00-real-world-patterns.md)
2. **Missing values** → [Pattern 14 (Null Handling)](02-data-operations/00-real-world-patterns.md)
3. **Type conversion** → [Pattern 13 (String Operations)](02-data-operations/00-real-world-patterns.md)
4. **Skewed distribution** → [Skew Problems](07-troubleshooting/03-skew-problems.md)

### Design Problems
1. **ETL pipeline design** → [System Design](08-interview-preparation/07-system-design.md)
2. **Production patterns** → [Production Patterns](06-production-patterns/)
3. **Data quality** → [Pattern 15 (Validation)](02-data-operations/00-real-world-patterns.md)
4. **Large-scale design** → [System Design](08-interview-preparation/07-system-design.md)

---

## 📚 Document Sizes & Time

| Document | Size | Read Time |
|----------|------|-----------|
| 00-quick-start.md | ~30 pages | 30 min |
| 01-spark-architecture.md | ~40 pages | 60 min |
| 02-dataframes-basics.md | ~25 pages | 45 min |
| 04-sparksql-basics.md | ~20 pages | 45 min |
| 00-real-world-patterns.md | ~60 pages | 2 hours |
| 01-caching-strategies.md | ~15 pages | 30 min |
| Troubleshooting (each) | ~15-20 pages | 20-30 min |
| Interview Q&A (each) | ~20-30 pages | 45-60 min |
| System Design | ~20 pages | 45 min |
| Cheatsheet | ~10 pages | Skim in 10 min |
| Glossary | ~15 pages | Lookup as needed |

---

## 🎯 Before You Start

Make sure you have:
- [ ] Python 3.8+ installed
- [ ] PySpark installed: `pip install pyspark`
- [ ] A text editor or Jupyter Notebook
- [ ] 30+ minutes available
- [ ] Willingness to run code

---

## 🚀 Recommended First Steps

### If you've never used Spark
```
1. Read: 00-START-HERE.md (10 min)
2. Read: 01-fundamentals/00-quick-start.md (20 min)
3. Run: First program (10 min)
4. Read: 01-fundamentals/01-spark-architecture.md (60 min)
5. Code along: Run examples from architecture guide (30 min)
→ Total: ~2.5 hours to understand Spark basics
```

### If you have Spark experience
```
1. Read: HANDBOOK_COMPLETE.md (10 min)
2. Skim: Real-World Patterns (30 min)
3. Practice: Do 5 coding problems (1 hour)
4. Bookmark: Cheatsheet and Glossary
→ Total: ~2 hours to get oriented
```

### If you're interviewing
```
1. Read: Interview README (15 min)
2. Work through: Core Concepts Q&A (1.5 hours)
3. Work through: DataFrame Q&A (1.5 hours)
4. Practice: 5 coding problems (1.5 hours)
5. Review: Cheatsheet day before (15 min)
→ Total: 5-6 hours for core prep
```

---

## 💡 Pro Tips

1. **Bookmark this page** - You'll come back often
2. **Print the cheatsheet** - Keep it at your desk
3. **Read glossary actively** - Look up terms you don't know
4. **Run every code example** - Don't just read
5. **Take notes** - Write key concepts in your own words
6. **Do exercises** - Reading alone isn't enough
7. **Reference troubleshooting** - Bookmark for emergencies

---

## 📞 Can't Find What You Need?

- **Concept question?** → Search [Glossary](09-reference/glossary.md)
- **Code example?** → Search [Cheatsheet](09-reference/cheatsheet.md)
- **Hit a real error?** → Search [Troubleshooting](07-troubleshooting/)
- **Interview question?** → Search [Interview Q&A](08-interview-preparation/)

---

## 🗺️ Handbook Map

```
00-START-HERE.md (Start here!)
    ↓
    ├── Beginner Path
    │   ├── 01-fundamentals/ (Weeks 1-2)
    │   ├── 02-data-operations/00-real-world-patterns.md (Week 3)
    │   └── exercises/ (Weeks 3-4)
    │
    ├── Interview Path
    │   ├── 08-interview-preparation/README.md (Plan)
    │   ├── 08-interview-preparation/01-05-qa.md (2-3 weeks)
    │   ├── 08-interview-preparation/06-08.md (1-2 weeks)
    │   └── 09-reference/cheatsheet.md (Day before)
    │
    └── Production Path
        ├── 01-fundamentals/01-spark-architecture.md (if needed)
        ├── 06-production-patterns/ (1 week)
        ├── 02-data-operations/00-real-world-patterns.md (1 week)
        ├── 04-performance-optimization/ (ongoing)
        ├── 07-troubleshooting/ (bookmarked!)
        └── 09-reference/cheatsheet.md (frequent reference)
```

---

## ✅ You're Ready!

Pick one:
1. **Learn Spark** → [Quick Start](01-fundamentals/00-quick-start.md)
2. **Ace an interview** → [Interview Guide](08-interview-preparation/README.md)
3. **Fix a problem** → [Troubleshooting](07-troubleshooting/)
4. **Master patterns** → [Real-World Patterns](02-data-operations/00-real-world-patterns.md)

**Start now. You've got this! 🚀**

---

**Last Updated**: May 2025
**Status**: ✅ Complete navigation system
**Version**: 1.0
