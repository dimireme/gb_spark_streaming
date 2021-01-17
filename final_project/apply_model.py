own_purchases = cass_baseline \
    .filter(F.col("name") == "random_5") \
    .select("list") \
    .rdd.flatMap(lambda x: x[0]) \
    .collect()