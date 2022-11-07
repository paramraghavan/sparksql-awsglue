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

Whole scale code geenration is not used :
- when using python UDF - user defined functions
- Exchange does not have whole-stage code generation because it is sending data across the network
- https://stackoverflow.com/questions/40554815/whole-stage-code-generation-in-spark-2-0

## Commonly used functions in Sparksession
![image](https://user-images.githubusercontent.com/52529498/200409366-62c8222f-0e18-42fa-adfa-51079e53a591.png)

### help spark
```python
help(spark)
```

```
Help on SparkSession in module pyspark.sql.session object:

class SparkSession(pyspark.sql.pandas.conversion.SparkConversionMixin)
 |  SparkSession(sparkContext, jsparkSession=None)
 |  
 |  The entry point to programming Spark with the Dataset and DataFrame API.
 |  
 |  A SparkSession can be used create :class:`DataFrame`, register :class:`DataFrame` as
 |  tables, execute SQL over tables, cache tables, and read parquet files.
 |  To create a :class:`SparkSession`, use the following builder pattern:
 |  
 |  .. autoattribute:: builder
 |     :annotation:
 |  
 |  Examples
 |  --------
 |  >>> spark = SparkSession.builder \
 |  ...     .master("local") \
 |  ...     .appName("Word Count") \
 |  ...     .config("spark.some.config.option", "some-value") \
 |  ...     .getOrCreate()
 |  
 |  >>> from datetime import datetime
 |  >>> from pyspark.sql import Row
 |  >>> spark = SparkSession(sc)
 |  >>> allTypes = sc.parallelize([Row(i=1, s="string", d=1.0, l=1,
 |  ...     b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
 |  ...     time=datetime(2014, 8, 1, 14, 1, 5))])
 |  >>> df = allTypes.toDF()
 |  >>> df.createOrReplaceTempView("allTypes")
 |  >>> spark.sql('select i+1, d+1, not b, list[1], dict["s"], time, row.a '
 |  ...            'from allTypes where b and i > 0').collect()
 |  [Row((i + 1)=2, (d + 1)=2.0, (NOT b)=False, list[1]=2,         dict[s]=0, time=datetime.datetime(2014, 8, 1, 14, 1, 5), a=1)]
 |  >>> df.rdd.map(lambda x: (x.i, x.s, x.d, x.l, x.b, x.time, x.row.a, x.list)).collect()
 |  [(1, 'string', 1.0, 1, True, datetime.datetime(2014, 8, 1, 14, 1, 5), 1, [1, 2, 3])]
 |  
 |  Method resolution order:
 |      SparkSession
 |      pyspark.sql.pandas.conversion.SparkConversionMixin
 |      builtins.object
 |  
 |  Methods defined here:
 |  
 |  __enter__(self)
 |      Enable 'with SparkSession.builder.(...).getOrCreate() as session: app' syntax.
 |      
 |      .. versionadded:: 2.0
 |  
 |  __exit__(self, exc_type, exc_val, exc_tb)
 |      Enable 'with SparkSession.builder.(...).getOrCreate() as session: app' syntax.
 |      
 |      Specifically stop the SparkSession on exit of the with block.
 |      
 |      .. versionadded:: 2.0
 |  
 |  __init__(self, sparkContext, jsparkSession=None)
 |      Initialize self.  See help(type(self)) for accurate signature.
 |  
 |  createDataFrame(self, data, schema=None, samplingRatio=None, verifySchema=True)
 |      Creates a :class:`DataFrame` from an :class:`RDD`, a list or a :class:`pandas.DataFrame`.
 |      
 |      When ``schema`` is a list of column names, the type of each column
 |      will be inferred from ``data``.
 |      
 |      When ``schema`` is ``None``, it will try to infer the schema (column names and types)
 |      from ``data``, which should be an RDD of either :class:`Row`,
 |      :class:`namedtuple`, or :class:`dict`.
 |      
 |      When ``schema`` is :class:`pyspark.sql.types.DataType` or a datatype string, it must match
 |      the real data, or an exception will be thrown at runtime. If the given schema is not
 |      :class:`pyspark.sql.types.StructType`, it will be wrapped into a
 |      :class:`pyspark.sql.types.StructType` as its only field, and the field name will be "value".
 |      Each record will also be wrapped into a tuple, which can be converted to row later.
 |      
 |      If schema inference is needed, ``samplingRatio`` is used to determined the ratio of
 |      rows used for schema inference. The first row will be used if ``samplingRatio`` is ``None``.
 |      
 |      .. versionadded:: 2.0.0
 |      
 |      .. versionchanged:: 2.1.0
 |         Added verifySchema.
 |      
 |      Parameters
 |      ----------
 |      data : :class:`RDD` or iterable
 |          an RDD of any kind of SQL data representation (:class:`Row`,
 |          :class:`tuple`, ``int``, ``boolean``, etc.), or :class:`list`, or
 |          :class:`pandas.DataFrame`.
 |      schema : :class:`pyspark.sql.types.DataType`, str or list, optional
 |          a :class:`pyspark.sql.types.DataType` or a datatype string or a list of
 |          column names, default is None.  The data type string format equals to
 |          :class:`pyspark.sql.types.DataType.simpleString`, except that top level struct type can
 |          omit the ``struct<>`` and atomic types use ``typeName()`` as their format, e.g. use
 |          ``byte`` instead of ``tinyint`` for :class:`pyspark.sql.types.ByteType`.
 |          We can also use ``int`` as a short name for :class:`pyspark.sql.types.IntegerType`.
 |      samplingRatio : float, optional
 |          the sample ratio of rows used for inferring
 |      verifySchema : bool, optional
 |          verify data types of every row against schema. Enabled by default.
 |      
 |      Returns
 |      -------
 |      :class:`DataFrame`
 |      
 |      Notes
 |      -----
 |      Usage with spark.sql.execution.arrow.pyspark.enabled=True is experimental.
 |      
 |      Examples
 |      --------
 |      >>> l = [('Alice', 1)]
 |      >>> spark.createDataFrame(l).collect()
 |      [Row(_1='Alice', _2=1)]
 |      >>> spark.createDataFrame(l, ['name', 'age']).collect()
 |      [Row(name='Alice', age=1)]
 |      
 |      >>> d = [{'name': 'Alice', 'age': 1}]
 |      >>> spark.createDataFrame(d).collect()
 |      [Row(age=1, name='Alice')]
 |      
 |      >>> rdd = sc.parallelize(l)
 |      >>> spark.createDataFrame(rdd).collect()
 |      [Row(_1='Alice', _2=1)]
 |      >>> df = spark.createDataFrame(rdd, ['name', 'age'])
 |      >>> df.collect()
 |      [Row(name='Alice', age=1)]
 |      
 |      >>> from pyspark.sql import Row
 |      >>> Person = Row('name', 'age')
 |      >>> person = rdd.map(lambda r: Person(*r))
 |      >>> df2 = spark.createDataFrame(person)
 |      >>> df2.collect()
 |      [Row(name='Alice', age=1)]
 |      
 |      >>> from pyspark.sql.types import *
 |      >>> schema = StructType([
 |      ...    StructField("name", StringType(), True),
 |      ...    StructField("age", IntegerType(), True)])
 |      >>> df3 = spark.createDataFrame(rdd, schema)
 |      >>> df3.collect()
 |      [Row(name='Alice', age=1)]
 |      
 |      >>> spark.createDataFrame(df.toPandas()).collect()  # doctest: +SKIP
 |      [Row(name='Alice', age=1)]
 |      >>> spark.createDataFrame(pandas.DataFrame([[1, 2]])).collect()  # doctest: +SKIP
 |      [Row(0=1, 1=2)]
 |      
 |      >>> spark.createDataFrame(rdd, "a: string, b: int").collect()
 |      [Row(a='Alice', b=1)]
 |      >>> rdd = rdd.map(lambda row: row[1])
 |      >>> spark.createDataFrame(rdd, "int").collect()
 |      [Row(value=1)]
 |      >>> spark.createDataFrame(rdd, "boolean").collect() # doctest: +IGNORE_EXCEPTION_DETAIL
 |      Traceback (most recent call last):
 |          ...
 |      Py4JJavaError: ...
 |  
 |  newSession(self)
 |      Returns a new :class:`SparkSession` as new session, that has separate SQLConf,
 |      registered temporary views and UDFs, but shared :class:`SparkContext` and
 |      table cache.
 |      
 |      .. versionadded:: 2.0
 |  
 |  range(self, start, end=None, step=1, numPartitions=None)
 |      Create a :class:`DataFrame` with single :class:`pyspark.sql.types.LongType` column named
 |      ``id``, containing elements in a range from ``start`` to ``end`` (exclusive) with
 |      step value ``step``.
 |      
 |      .. versionadded:: 2.0.0
 |      
 |      Parameters
 |      ----------
 |      start : int
 |          the start value
 |      end : int, optional
 |          the end value (exclusive)
 |      step : int, optional
 |          the incremental step (default: 1)
 |      numPartitions : int, optional
 |          the number of partitions of the DataFrame
 |      
 |      Returns
 |      -------
 |      :class:`DataFrame`
 |      
 |      Examples
 |      --------
 |      >>> spark.range(1, 7, 2).collect()
 |      [Row(id=1), Row(id=3), Row(id=5)]
 |      
 |      If only one argument is specified, it will be used as the end value.
 |      
 |      >>> spark.range(3).collect()
 |      [Row(id=0), Row(id=1), Row(id=2)]
 |  
 |  sql(self, sqlQuery)
 |      Returns a :class:`DataFrame` representing the result of the given query.
 |      
 |      .. versionadded:: 2.0.0
 |      
 |      Returns
 |      -------
 |      :class:`DataFrame`
 |      
 |      Examples
 |      --------
 |      >>> df.createOrReplaceTempView("table1")
 |      >>> df2 = spark.sql("SELECT field1 AS f1, field2 as f2 from table1")
 |      >>> df2.collect()
 |      [Row(f1=1, f2='row1'), Row(f1=2, f2='row2'), Row(f1=3, f2='row3')]
 |  
 |  stop(self)
 |      Stop the underlying :class:`SparkContext`.
 |      
 |      .. versionadded:: 2.0
 |  
 |  table(self, tableName)
 |      Returns the specified table as a :class:`DataFrame`.
 |      
 |      .. versionadded:: 2.0.0
 |      
 |      Returns
 |      -------
 |      :class:`DataFrame`
 |      
 |      Examples
 |      --------
 |      >>> df.createOrReplaceTempView("table1")
 |      >>> df2 = spark.table("table1")
 |      >>> sorted(df.collect()) == sorted(df2.collect())
 |      True
 |  
 |  ----------------------------------------------------------------------
 |  Class methods defined here:
 |  
 |  getActiveSession() from builtins.type
 |      Returns the active :class:`SparkSession` for the current thread, returned by the builder
 |      
 |      .. versionadded:: 3.0.0
 |      
 |      Returns
 |      -------
 |      :class:`SparkSession`
 |          Spark session if an active session exists for the current thread
 |      
 |      Examples
 |      --------
 |      >>> s = SparkSession.getActiveSession()
 |      >>> l = [('Alice', 1)]
 |      >>> rdd = s.sparkContext.parallelize(l)
 |      >>> df = s.createDataFrame(rdd, ['name', 'age'])
 |      >>> df.select("age").collect()
 |      [Row(age=1)]
 |  
 |  ----------------------------------------------------------------------
 |  Readonly properties defined here:
 |  
 |  catalog
 |      Interface through which the user may create, drop, alter or query underlying
 |      databases, tables, functions, etc.
 |      
 |      .. versionadded:: 2.0.0
 |      
 |      Returns
 |      -------
 |      :class:`Catalog`
 |  
 |  conf
 |      Runtime configuration interface for Spark.
 |      
 |      This is the interface through which the user can get and set all Spark and Hadoop
 |      configurations that are relevant to Spark SQL. When getting the value of a config,
 |      this defaults to the value set in the underlying :class:`SparkContext`, if any.
 |      
 |      Returns
 |      -------
 |      :class:`pyspark.sql.conf.RuntimeConfig`
 |      
 |      .. versionadded:: 2.0
 |  
 |  read
 |      Returns a :class:`DataFrameReader` that can be used to read data
 |      in as a :class:`DataFrame`.
 |      
 |      .. versionadded:: 2.0.0
 |      
 |      Returns
 |      -------
 |      :class:`DataFrameReader`
 |  
 |  readStream
 |      Returns a :class:`DataStreamReader` that can be used to read data streams
 |      as a streaming :class:`DataFrame`.
 |      
 |      .. versionadded:: 2.0.0
 |      
 |      Notes
 |      -----
 |      This API is evolving.
 |      
 |      Returns
 |      -------
 |      :class:`DataStreamReader`
 |  
 |  sparkContext
 |      Returns the underlying :class:`SparkContext`.
 |      
 |      .. versionadded:: 2.0
 |  
 |  streams
 |      Returns a :class:`StreamingQueryManager` that allows managing all the
 |      :class:`StreamingQuery` instances active on `this` context.
 |      
 |      .. versionadded:: 2.0.0
 |      
 |      Notes
 |      -----
 |      This API is evolving.
 |      
 |      Returns
 |      -------
 |      :class:`StreamingQueryManager`
 |  
 |  udf
 |      Returns a :class:`UDFRegistration` for UDF registration.
 |      
 |      .. versionadded:: 2.0.0
 |      
 |      Returns
 |      -------
 |      :class:`UDFRegistration`
 |  
 |  version
 |      The version of Spark on which this application is running.
 |      
 |      .. versionadded:: 2.0
 |  
 |  ----------------------------------------------------------------------
 |  Data and other attributes defined here:
 |  
 |  Builder = <class 'pyspark.sql.session.SparkSession.Builder'>
 |      Builder for :class:`SparkSession`.
 |  
 |  
 |  builder = <pyspark.sql.session.SparkSession.Builder object>
 |  
 |  ----------------------------------------------------------------------
 |  Data descriptors inherited from pyspark.sql.pandas.conversion.SparkConversionMixin:
 |  
 |  __dict__
 |      dictionary for instance variables (if defined)
 |  
 |  __weakref__
 |      list of weak references to the object (if defined)
```

```python
help(spark.udf)
```

```
Help on UDFRegistration in module pyspark.sql.udf object:

class UDFRegistration(builtins.object)
 |  UDFRegistration(sparkSession)
 |  
 |  Wrapper for user-defined function registration. This instance can be accessed by
 |  :attr:`spark.udf` or :attr:`sqlContext.udf`.
 |  
 |  .. versionadded:: 1.3.1
 |  
 |  Methods defined here:
 |  
 |  __init__(self, sparkSession)
 |      Initialize self.  See help(type(self)) for accurate signature.
 |  
 |  register(self, name, f, returnType=None)
 |      Register a Python function (including lambda function) or a user-defined function
 |      as a SQL function.
 |      
 |      .. versionadded:: 1.3.1
 |      
 |      Parameters
 |      ----------
 |      name : str,
 |          name of the user-defined function in SQL statements.
 |      f : function, :meth:`pyspark.sql.functions.udf` or :meth:`pyspark.sql.functions.pandas_udf`
 |          a Python function, or a user-defined function. The user-defined function can
 |          be either row-at-a-time or vectorized. See :meth:`pyspark.sql.functions.udf` and
 |          :meth:`pyspark.sql.functions.pandas_udf`.
 |      returnType : :class:`pyspark.sql.types.DataType` or str, optional
 |          the return type of the registered user-defined function. The value can
 |          be either a :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
 |          `returnType` can be optionally specified when `f` is a Python function but not
 |          when `f` is a user-defined function. Please see the examples below.
 |      
 |      Returns
 |      -------
 |      function
 |          a user-defined function
 |      
 |      Notes
 |      -----
 |      To register a nondeterministic Python function, users need to first build
 |      a nondeterministic user-defined function for the Python function and then register it
 |      as a SQL function.
 |      
 |      Examples
 |      --------
 |      1. When `f` is a Python function:
 |      
 |          `returnType` defaults to string type and can be optionally specified. The produced
 |          object must match the specified type. In this case, this API works as if
 |          `register(name, f, returnType=StringType())`.
 |      
 |          >>> strlen = spark.udf.register("stringLengthString", lambda x: len(x))
 |          >>> spark.sql("SELECT stringLengthString('test')").collect()
 |          [Row(stringLengthString(test)='4')]
 |      
 |          >>> spark.sql("SELECT 'foo' AS text").select(strlen("text")).collect()
 |          [Row(stringLengthString(text)='3')]
 |      
 |          >>> from pyspark.sql.types import IntegerType
 |          >>> _ = spark.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
 |          >>> spark.sql("SELECT stringLengthInt('test')").collect()
 |          [Row(stringLengthInt(test)=4)]
 |      
 |          >>> from pyspark.sql.types import IntegerType
 |          >>> _ = spark.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
 |          >>> spark.sql("SELECT stringLengthInt('test')").collect()
 |          [Row(stringLengthInt(test)=4)]
 |      
 |      2. When `f` is a user-defined function (from Spark 2.3.0):
 |      
 |          Spark uses the return type of the given user-defined function as the return type of
 |          the registered user-defined function. `returnType` should not be specified.
 |          In this case, this API works as if `register(name, f)`.
 |      
 |          >>> from pyspark.sql.types import IntegerType
 |          >>> from pyspark.sql.functions import udf
 |          >>> slen = udf(lambda s: len(s), IntegerType())
 |          >>> _ = spark.udf.register("slen", slen)
 |          >>> spark.sql("SELECT slen('test')").collect()
 |          [Row(slen(test)=4)]
 |      
 |          >>> import random
 |          >>> from pyspark.sql.functions import udf
 |          >>> from pyspark.sql.types import IntegerType
 |          >>> random_udf = udf(lambda: random.randint(0, 100), IntegerType()).asNondeterministic()
 |          >>> new_random_udf = spark.udf.register("random_udf", random_udf)
 |          >>> spark.sql("SELECT random_udf()").collect()  # doctest: +SKIP
 |          [Row(random_udf()=82)]
 |      
 |          >>> import pandas as pd  # doctest: +SKIP
 |          >>> from pyspark.sql.functions import pandas_udf
 |          >>> @pandas_udf("integer")  # doctest: +SKIP
 |          ... def add_one(s: pd.Series) -> pd.Series:
 |          ...     return s + 1
 |          ...
 |          >>> _ = spark.udf.register("add_one", add_one)  # doctest: +SKIP
 |          >>> spark.sql("SELECT add_one(id) FROM range(3)").collect()  # doctest: +SKIP
 |          [Row(add_one(id)=1), Row(add_one(id)=2), Row(add_one(id)=3)]
 |      
 |          >>> @pandas_udf("integer")  # doctest: +SKIP
 |          ... def sum_udf(v: pd.Series) -> int:
 |          ...     return v.sum()
 |          ...
 |          >>> _ = spark.udf.register("sum_udf", sum_udf)  # doctest: +SKIP
 |          >>> q = "SELECT sum_udf(v1) FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2"
 |          >>> spark.sql(q).collect()  # doctest: +SKIP
 |          [Row(sum_udf(v1)=1), Row(sum_udf(v1)=5)]
 |  
 |  registerJavaFunction(self, name, javaClassName, returnType=None)
 |      Register a Java user-defined function as a SQL function.
 |      
 |      In addition to a name and the function itself, the return type can be optionally specified.
 |      When the return type is not specified we would infer it via reflection.
 |      
 |      .. versionadded:: 2.3.0
 |      
 |      Parameters
 |      ----------
 |      name : str
 |          name of the user-defined function
 |      javaClassName : str
 |          fully qualified name of java class
 |      returnType : :class:`pyspark.sql.types.DataType` or str, optional
 |          the return type of the registered Java function. The value can be either
 |          a :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
 |      
 |      Examples
 |      --------
 |      >>> from pyspark.sql.types import IntegerType
 |      >>> spark.udf.registerJavaFunction(
 |      ...     "javaStringLength", "test.org.apache.spark.sql.JavaStringLength", IntegerType())
 |      ... # doctest: +SKIP
 |      >>> spark.sql("SELECT javaStringLength('test')").collect()  # doctest: +SKIP
 |      [Row(javaStringLength(test)=4)]
 |      
 |      >>> spark.udf.registerJavaFunction(
 |      ...     "javaStringLength2", "test.org.apache.spark.sql.JavaStringLength")
 |      ... # doctest: +SKIP
 |      >>> spark.sql("SELECT javaStringLength2('test')").collect()  # doctest: +SKIP
 |      [Row(javaStringLength2(test)=4)]
 |      
 |      >>> spark.udf.registerJavaFunction(
 |      ...     "javaStringLength3", "test.org.apache.spark.sql.JavaStringLength", "integer")
 |      ... # doctest: +SKIP
 |      >>> spark.sql("SELECT javaStringLength3('test')").collect()  # doctest: +SKIP
 |      [Row(javaStringLength3(test)=4)]
 |  
 |  registerJavaUDAF(self, name, javaClassName)
 |      Register a Java user-defined aggregate function as a SQL function.
 |      
 |      .. versionadded:: 2.3.0
 |      
 |      name : str
 |          name of the user-defined aggregate function
 |      javaClassName : str
 |          fully qualified name of java class
 |      
 |      Examples
 |      --------
 |      >>> spark.udf.registerJavaUDAF("javaUDAF", "test.org.apache.spark.sql.MyDoubleAvg")
 |      ... # doctest: +SKIP
 |      >>> df = spark.createDataFrame([(1, "a"),(2, "b"), (3, "a")],["id", "name"])
 |      >>> df.createOrReplaceTempView("df")
 |      >>> q = "SELECT name, javaUDAF(id) as avg from df group by name order by name desc"
 |      >>> spark.sql(q).collect()  # doctest: +SKIP
 |      [Row(name='b', avg=102.0), Row(name='a', avg=102.0)]
 |  
 |  ----------------------------------------------------------------------
 |  Data descriptors defined here:
 |  
 |  __dict__
 |      dictionary for instance variables (if defined)
 |  
 |  __weakref__
 |      list of weak references to the object (if defined)
```

![image](https://user-images.githubusercontent.com/52529498/200436581-ef9247e4-616d-4cef-844a-dc6e7264a299.png)
