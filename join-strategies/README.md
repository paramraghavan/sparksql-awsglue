# Spark Join Strategies 

## Broadcast Hash Join
![img_1.png](img_1.png)
- Here in this case we create a hashmap of the smaller table/relation based on the join key.
- then loop over the larger table/relation map the join key with the hashtable created above and pull out the matching value 

Hash Join is performed by first creating a Hash Table based on join_key of smaller relation and then looping over larger 
relation to match the hashed join_key values. Also, this is only supported for ‘=’ join.

![img.png](img.png)

In broadcast hash join, copy of one of the join relations/hashmap are being sent to all the worker nodes and it saves
shuffling cost. This is useful when you are joining a large relation with a smaller one. It is also known as map-side 
join(associating worker nodes with mappers).

Spark deploys this join strategy when the size of one of the **join relations is less than the threshold values(default 10 M).**
The spark property which defines this threshold is spark.sql.autoBroadcastJoinThreshold(configurable).

Broadcast relations are shared among executors using the BitTorrent protocol(read more here). It is a peer 
to peer protocol in which block of files can be shared by peers amongst each other. Hence, they don’t need to 
rely on a single node. This is how peer to peer protocol works:

![img_2.png](img_2.png)


**Things to Note:**

- The broadcasted relation/hashmap should fit completely into the memory of each executor as well as the driver. The hashmap is created in the Driver, 
   driver will send it to the executors.
- Only supported for ‘=’ join.
- Supported for all join types(inner, left, right) except full outer joins.
- When the broadcast size is small, it is usually faster than other join strategies.
- Copy of relation is broadcasted over the network. Therefore, being a network-intensive operation
  could cause out of memory errors or performance issues when broadcast size is big(for instance,
  when explicitly specified to use broadcast join or change the default threshold).
- You can’t make changes to the broadcasted relation, after broadcast. Even if you do, they won’t be available to 
  the worker nodes(because the copy is already shipped).
  
