# Behavioral Questions & Scenarios

10 real production problems and how you handled them.

## Scenario 1: Job Taking Too Long

**Your answer** (STAR format):
- **Situation**: Daily ETL running 4 hours, blocking downstream
- **Task**: Optimize to meet SLA
- **Action**: 
  1. Checked Spark UI → found task skew
  2. Analyzed data → one customer had 40% of transactions
  3. Implemented salting technique
  4. Tested on 10% sample, then deployed
- **Result**: 12 minutes (20x faster), unblocked BI team

## Scenario 2: Debugging Production OutageSTAR

**Your answer**:
- **Situation**: Job failed at 3am, data delayed
- **Task**: Root cause and quick fix
- **Action**:
  1. Checked Spark UI and logs
  2. Found OOM error in one executor
  3. Temporarily increased executor memory
  4. Job completed, investigated root cause next day
- **Result**: Implemented partition increase, prevents future failures

More scenarios: Prepare 5-7 of your own from work experience!
