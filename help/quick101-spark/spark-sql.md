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
- 
