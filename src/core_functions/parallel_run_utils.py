from writers.DataWriterFactory import DataWriterFactory
import concurrent.futures
import itertools


def parallel_run(function, items, apply_flat_map=False):
    """
    Invoke the parallel execution of a function
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(function, items)
        results = list(results)
        if apply_flat_map:
            results = list(itertools.chain(*results))
        return results


def get_writer_and_write(entity_config, source_df, spark, dbutils, logger):
    """
    Identify the writer type and write to delta table
    """
    # Initialize data write from DataWriterFactory
    data_writer = DataWriterFactory.get_datawriter(entity_config, spark, dbutils, logger)
    # Write to delta table
    data_writer.write_to_target_table(source_df)


def get_writer_and_write_wrapper(args):
    """
    Wrapper function to pass arguments to get_writer_and_write
    """
    return get_writer_and_write(*args)
