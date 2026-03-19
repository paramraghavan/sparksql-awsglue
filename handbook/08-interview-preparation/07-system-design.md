# System Design Questions

10 architecture scenarios for senior-level interviews.

## Question 1: Design an ETL pipeline for a data lake

**Answer structure**:
1. **Requirements clarification**:
   - Data volume? Format? Frequency?
   - SLA (latency, availability)?
   - Team size?

2. **Architecture**:
   - Ingest → Process → Store → Query
   - Technologies: AWS Glue, S3, Spark, Athena

3. **Challenges**:
   - Data skew handling
   - Incremental processing
   - Monitoring

## Question 2: Monitor Spark jobs in production

**Answer**: 
- Spark UI (4040 port)
- Event logs to S3
- Custom metrics (Spark listeners)
- CloudWatch alarms

## Question 3: Handle data skew in production

**Answer**: Salting + AQE + monitoring

More scenarios in [08-behavioral-scenarios.md](08-behavioral-scenarios.md)
