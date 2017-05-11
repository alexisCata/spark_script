### IFACE EXTRACTIONS ###

This project is in charge of the daily booking extracctions. 
It connects to a Redshift database using Psycopg2 and Pyspark. It queries de db, retrieves data, makes some transformations and returns it as ...

* Version: 0.1

### Dependencies ###

* [Pyspark](https://spark.apache.org/)
* [Psycopg2](https://pypi.python.org/pypi/psycopg2)

### How do I run it? ###

```shell
/bin/spark-submit --class ./iface_extractions/query arg_date1 arg_date2
```
