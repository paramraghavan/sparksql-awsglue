## Abstraction of user program
![image](https://user-images.githubusercontent.com/52529498/200226686-e9cc4a00-e755-4ca1-9c83-4c957cc008a5.png)

- The spark compiler will first identify expressions in the user program - spark sql or data frame api. In the above picture expressions
 are highlighted in green.

## Query plan
Describe data operation like aggregates, joins, filters etc. and these operations essentially generate a new dataset
based on a input dataset.

![image](https://user-images.githubusercontent.com/52529498/200227272-24590c4a-af2b-4e46-8408-160e5b3d9113.png)
- In the above picture query plan is highlighted in green
- First step table t1 and t2 are read/scanned
- next t1 and t2 are joined
- third step filter conditon, where is applied
- fourth step projection is applied using select.
- finally aggregate is applied.

### Logical Plan
![image](https://user-images.githubusercontent.com/52529498/200234055-80c1186f-435b-42c5-ab6c-b0b7794242fa.png)
Logical plan only talk about scan does not identify the type of scan, similary does not identify the type of join and so on, b ut physcial plan will identify all these details

## Physical Plan
![image](https://user-images.githubusercontent.com/52529498/200234419-ec8c8e11-8f26-40b9-84fa-618c69ba6171.png)


## Transform
![image](https://user-images.githubusercontent.com/52529498/200381131-7b3d9ecb-9313-499a-aa7a-46ec6add108d.png)
- in the above picture adding 1+2 and using as value 3 everywhere is called **constant folding**, there are 100 of other techniques - these in turn improve performance.
- Predicate pushdown, the filter is applied at the table source and not after the tables are joined
![image](https://user-images.githubusercontent.com/52529498/200387765-8092d348-2280-4bdb-bc6a-85d21bc0c81e.png)
- Constant folding
![image](https://user-images.githubusercontent.com/52529498/200388083-d7cbec40-a613-46b0-b865-e11e5ff64874.png)
see t2.id> 50*1000 --> to t2.id > 50000 and 1+2+t1.value as  3+t1.value.
- Column pruning
![image](https://user-images.githubusercontent.com/52529498/200391179-71b6b835-2784-416e-bdae-72fcb80059f0.png)
When the table t1 and t2 are read all the columns are not read, but only the columns used are read, rest are pruned - not read from datasource

## Execution Plan
- explain(extended=False):
Prints the (logical and physical) plans to the console for debugging purpose.
Extended is a Boolean parameter.default : False. If False, it prints only the physical plan.
If True, it prints all - Parsed Logical Plan, Analyzed Logical Plan, Optimized Logical Plan and Physical Plan.
Explain function is extended for whole-stage code generation. When an operator has a star around it (*), whole-stage code generation is enabled. In the following example, Range, Filter, and the two Aggregates are both running with whole-stage code generation. Exchange does not
have whole-stage code generation because it is sending data across the network. spark.sql.codegen.wholeStage is enabled by default for spark 2.0. and above, it will do all the internal optimization possible from the spark catalist side .
Example:
```
spark.conf.set("spark.sql.codegen.wholeStage",True)
spark.range(1000).filter("id > 100").selectExpr("sum(id)").explain()

== Physical Plan ==
*(2) HashAggregate(keys=[], functions=[sum(id#23L)])
+- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#62]
   +- *(1) HashAggregate(keys=[], functions=[partial_sum(id#23L)])
      +- *(1) Filter (id#23L > 100)
         +- *(1) Range (0, 1000, step=1, splits=8)

```

```
spark.range(1000).filter("id > 100").selectExpr("sum(id)").explain(extended=True)

== Parsed Logical Plan ==
'Project [unresolvedalias('sum('id), Some(org.apache.spark.sql.Column$$Lambda$2515/626237529@744977fb))]
+- Filter (id#32L > cast(100 as bigint))
   +- Range (0, 1000, step=1, splits=Some(8))

== Analyzed Logical Plan ==
sum(id): bigint
Aggregate [sum(id#32L) AS sum(id)#37L]
+- Filter (id#32L > cast(100 as bigint))
   +- Range (0, 1000, step=1, splits=Some(8))

== Optimized Logical Plan ==
Aggregate [sum(id#32L) AS sum(id)#37L]
+- Filter (id#32L > 100)
   +- Range (0, 1000, step=1, splits=Some(8))

== Physical Plan ==
*(2) HashAggregate(keys=[], functions=[sum(id#32L)], output=[sum(id)#37L])
+- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#83]
   +- *(1) HashAggregate(keys=[], functions=[partial_sum(id#32L)], output=[sum#40L])
      +- *(1) Filter (id#32L > 100)
         +- *(1) Range (0, 1000, step=1, splits=8)
```
