# spark plugin

- pyspark transformations applied via plugin  defined by implementing an interface 
- interface defines the following contract
  - read, reads the data
  - transform, applies tranformation rules
  - write, writes data
  - this interface takes the sparkcontext 
  - add decorators to all the abstract methods
- [See here for loading classes by name or by module name](https://github.com/paramraghavan/beginners-py-learn/tree/main/src/advance_stuff/patterns/classforname)