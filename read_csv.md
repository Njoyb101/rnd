# Sample data
```bash
ID|date|customerId|customerName|address|wishlisted
1|2024-10-01 12:00:00-04:00|1001|John Doe|["123 Elm St", "Apt 4B", "Springfield, IL"]|{"item1": true, "item2": false}
2|  2024-10-02 13:30:00-04:00 | 1002 | Jane Smith | ["456 Oak St", "Townsville, TX"] | { "item3": true }
3|2024-10-03 09:15:00-04:00|1003|Alice Johnson|["789 Pine St"]|{"item4": false, "item5": true, "item6": true}
4|2024-10-04 16:45:00-04:00|1004|Bob Brown|["135 Maple Ave", "Cityville, NY"]|{}
5|2024-10-05 08:00:00-04:00|1005|Charlie Davis|null|{"item7": true}
6|2024-10-06 10:30:00-04:00|1006|Invalid Customer|["Invalid Address"]|{"item8": true, "item9": "not_a_boolean"}
```

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, MapType, BooleanType

mySchema = StructType([
    StructField("ID", IntegerType(), nullable=False),
    StructField("date", TimestampType(), nullable=True),
    StructField("customerId", IntegerType(), nullable=False),
    StructField("customerName", StringType(), nullable=True),
    StructField("address", ArrayType(StringType()), nullable=True),
    StructField("wishlisted", MapType(StringType(), BooleanType()), nullable=True),
])

df = (spark.read
        .format("csv")
        .option("header", "true")
        .option("sep","|")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("quote", '\"')
        .option("escape", "\'")
        .option("timestampFormat","yyyy-MM-dd HH:mm:ss-04:00")
        .option("schema", mySchema)
        .option("mode","DROPMALFORMED")
        .load(file))
    
df.show(truncate=False)

```
