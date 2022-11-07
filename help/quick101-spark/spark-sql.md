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
- in the above picture adding 1+2 and using as value 3 everywhere is called **constant folding**, there are 100 of other techniques that are used.
- Predicate pushdown, the filter is applied at the table source and not after the tables are joined
![image](https://user-images.githubusercontent.com/52529498/200387765-8092d348-2280-4bdb-bc6a-85d21bc0c81e.png)
- Constant folding
![image](https://user-images.githubusercontent.com/52529498/200388083-d7cbec40-a613-46b0-b865-e11e5ff64874.png)
see t2.id> 50*1000 --> to t2.id > 50000 and 1+2+t1.value as  3+t1.value.
- Column pruning
![image](https://user-images.githubusercontent.com/52529498/200391179-71b6b835-2784-416e-bdae-72fcb80059f0.png)
When the table t1 and t2 are read all the columns are not read, but only the columns used are read, rest are pruned - not read from datasource


