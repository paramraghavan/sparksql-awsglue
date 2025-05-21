Dropping columns on  a dataframe is taking too long, see below, how to fix it
df.drop(*[var for var in df.columns if '_raw' in var  or 'num_conds_rule' in var])