from pyspark.sql import DataFrame


def assert_dataframes_are_equal(left: DataFrame, right: DataFrame) -> bool:
    """
    Some transformations create not nullable columns (`nullable` = False).
    .simpleString() allows us to assert that the left and right
    DDL formatted strings are the same
    e.g. struct<id:int,name:string,date:date>
    """
    assert left.schema.simpleString() == right.schema.simpleString()
    assert left.collect() == right.collect()
