# ETL Framework

## Interfaces

 ### IDataTransform.py 
 This file contains the definition of the IDataTransform interface. This interface provides a blueprint for implementing data transformation classes. It includes two abstract methods: init and apply_transform.

The init method initializes the IDataTransform object with the source DataFrame, entity configuration, SparkSession, and dbutils. 
The apply_transform method takes the source DataFrame, entity configuration, SparkSession, and dbutils as inputs and applies the transformation to the data, returning a dictionary of transformed DataFrames.

This interface will serve as a foundation for implementing concrete data transformation classes in the ETL framework.