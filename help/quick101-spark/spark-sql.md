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
Logical plan opnly talk about scan does not identify the type of scan, similary does not identify the type of join and so on, b ut physcial plan will identify all these details

## Physical Plan
![image](https://user-images.githubusercontent.com/52529498/200234419-ec8c8e11-8f26-40b9-84fa-618c69ba6171.png)


