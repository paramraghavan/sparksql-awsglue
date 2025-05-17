A custom EMR cluster scaling configuration file that includes all the elements you mentioned. Here's a breakdown of the
key components:

### Core Configuration:

- Basic EMR cluster setup with Spark and Hadoop applications
- Standard master and core node configuration

### Custom Scaling Features:

1. **Number of servers to scale to**:
    - Each application has its own `MaxCapacity` and `MinCapacity` settings
    - For example, `data-processing-app` can scale from 2 to 20 nodes

2. **Task node mix (spot vs on-demand)**:
    - Each app defines its preferred instance mix
    - For example, `data-processing-app` uses 80% spot instances and 20% on-demand
    - `ml-training-app` uses a higher percentage of on-demand (40%) for more stability

3. **Instance count and type assignment by app**:
    - Each application has specific instance types allocated
    - For example, `data-processing-app` uses memory-optimized instances (r5 family)
    - `ml-training-app` uses GPU instances (p3 and g4dn families)

### Additional Features:

- Scaling triggers based on YARN memory availability and container pending ratio
- Timeout settings that switch to on-demand if spot instances aren't available
- Tagging for cost allocation

This configuration gives you a flexible setup that will automatically adjust resources based on the specific Spark
application (appName) that's running. You can easily modify the instance types, counts, and spot/on-demand ratios for
each application type.

