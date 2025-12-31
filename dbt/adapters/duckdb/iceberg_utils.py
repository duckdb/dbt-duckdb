"""
Iceberg schema evolution utilities using PyIceberg for AWS S3 Tables
"""
from typing import List, Dict, Any, Optional, Union
import logging

logger = logging.getLogger(__name__)


class IcebergSchemaEvolution:
    """Handle schema evolution for Iceberg tables using PyIceberg"""
    
    def __init__(self, catalog_config: Dict[str, Any]):
        """
        Initialize with catalog configuration
        
        Args:
            catalog_config: Configuration for PyIceberg catalog
        """
        try:
            from pyiceberg.catalog import load_catalog
            self.catalog = load_catalog('s3_tables', **catalog_config)
        except ImportError:
            raise ImportError(
                "PyIceberg is required for schema evolution on S3 Tables. "
                "Install it with: pip install pyiceberg"
            )
    
    def add_columns(self, table_identifier: str, columns: List[Dict[str, str]]) -> bool:
        """
        Add new columns to an Iceberg table
        
        Args:
            table_identifier: Full table identifier (e.g., 'namespace.table_name')
            columns: List of column definitions
                Example: [
                    {'name': 'new_col1', 'type': 'VARCHAR'},
                    {'name': 'new_col2', 'type': 'INTEGER'}
                ]
        
        Returns:
            True if successful
        """
        try:
            table = self.catalog.load_table(table_identifier)
            
            with table.update_schema() as update:
                for col in columns:
                    col_type = self._map_type(col['type'])
                    logger.info(f"Adding column {col['name']} with type {col['type']}")
                    update.add_column(col['name'], col_type)
            
            logger.info(f"Successfully added {len(columns)} columns to {table_identifier}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add columns to {table_identifier}: {e}")
            raise
    
    def drop_columns(self, table_identifier: str, column_names: List[str]) -> bool:
        """
        Drop columns from an Iceberg table
        
        Note: This method exists for completeness but should not be used.
        Columns removed from source are preserved in target with NULL values.
        
        Args:
            table_identifier: Full table identifier
            column_names: List of column names to drop
        
        Returns:
            True if successful
        """
        try:
            table = self.catalog.load_table(table_identifier)
            
            with table.update_schema() as update:
                for col_name in column_names:
                    logger.info(f"Dropping column {col_name}")
                    update.delete_column(col_name)
            
            logger.info(f"Successfully dropped {len(column_names)} columns from {table_identifier}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop columns from {table_identifier}: {e}")
            raise
    
    def _map_type(self, duckdb_type: str):
        """Map DuckDB type string to PyIceberg type"""
        from pyiceberg.types import (
            StringType, IntegerType, LongType, FloatType, DoubleType,
            BooleanType, DateType, TimestampType, DecimalType, BinaryType
        )
        
        type_upper = duckdb_type.upper().strip()
        
        # Handle parameterized types
        if type_upper.startswith('DECIMAL'):
            return DecimalType(38, 9)
        
        if type_upper.startswith('VARCHAR') or type_upper.startswith('CHAR'):
            return StringType()
        
        # Simple type mapping
        type_mapping = {
            'VARCHAR': StringType(),
            'TEXT': StringType(),
            'STRING': StringType(),
            'CHAR': StringType(),
            'INTEGER': IntegerType(),
            'INT': IntegerType(),
            'INT4': IntegerType(),
            'SMALLINT': IntegerType(),
            'INT2': IntegerType(),
            'TINYINT': IntegerType(),
            'INT1': IntegerType(),
            'BIGINT': LongType(),
            'INT8': LongType(),
            'HUGEINT': LongType(),
            'FLOAT': FloatType(),
            'FLOAT4': FloatType(),
            'REAL': FloatType(),
            'DOUBLE': DoubleType(),
            'FLOAT8': DoubleType(),
            'NUMERIC': DecimalType(38, 9),
            'BOOLEAN': BooleanType(),
            'BOOL': BooleanType(),
            'DATE': DateType(),
            'TIMESTAMP': TimestampType(),
            'TIMESTAMPTZ': TimestampType(),
            'DATETIME': TimestampType(),
            'BLOB': BinaryType(),
            'BYTEA': BinaryType(),
            'BINARY': BinaryType(),
            'VARBINARY': BinaryType(),
        }
        
        mapped_type = type_mapping.get(type_upper, StringType())
        logger.debug(f"Mapped DuckDB type '{duckdb_type}' to PyIceberg type '{mapped_type}'")
        return mapped_type
    
    def update_partition_spec(self, table_identifier: str, partition_specs: List[str]) -> bool:
        """
        Update partition spec for an Iceberg table
        
        Note: This adds partition fields to an existing table. For best results,
        the table should be empty or newly created.
        
        AWS S3 Tables Limitation: bucket and truncate transforms are not supported.
        Other transforms (identity, day, month, year, hour) are supported.
        
        Args:
            table_identifier: Full table identifier (e.g., 'namespace.table_name')
            partition_specs: List of partition specifications
                Examples:
                - ['country', 'region']  # Identity partitions
                - ['day(order_date)', 'country']  # Day transform + identity
                - ['year(order_date)', 'month(order_date)']  # Year and month transforms
        
        Returns:
            True if successful
        """
        try:
            from pyiceberg.transforms import (
                DayTransform, MonthTransform, YearTransform, HourTransform,
                BucketTransform, TruncateTransform, IdentityTransform
            )
            
            logger.info(f"Loading table {table_identifier} from catalog")
            table = self.catalog.load_table(table_identifier)
            logger.info(f"Table loaded successfully. Current schema: {table.schema()}")
            
            with table.update_spec() as update:
                for spec in partition_specs:
                    spec = spec.strip()
                    logger.info(f"Processing partition spec: {spec}")
                    
                    # Parse partition transform
                    if '(' in spec and ')' in spec:
                        # Transform function: day(col), month(col), year(col), etc.
                        transform_name = spec[:spec.index('(')].lower()
                        args_str = spec[spec.index('(')+1:spec.rindex(')')]
                        args = [arg.strip() for arg in args_str.split(',')]
                        
                        source_column = args[0]
                        
                        # Validate column exists in schema
                        schema_fields = {field.name.lower(): field.name for field in table.schema().fields}
                        if source_column.lower() not in schema_fields:
                            available_cols = ', '.join(schema_fields.values())
                            raise ValueError(
                                f"Column '{source_column}' not found in table schema. "
                                f"Available columns: {available_cols}"
                            )
                        
                        # Use the actual column name from schema (case-sensitive)
                        actual_column_name = schema_fields[source_column.lower()]
                        
                        if transform_name == 'day':
                            logger.info(f"Adding day partition on {actual_column_name}")
                            update.add_field(actual_column_name, DayTransform(), f"{actual_column_name}_day")
                        elif transform_name == 'month':
                            logger.info(f"Adding month partition on {actual_column_name}")
                            update.add_field(actual_column_name, MonthTransform(), f"{actual_column_name}_month")
                        elif transform_name == 'year':
                            logger.info(f"Adding year partition on {actual_column_name}")
                            update.add_field(actual_column_name, YearTransform(), f"{actual_column_name}_year")
                        elif transform_name == 'hour':
                            logger.info(f"Adding hour partition on {actual_column_name}")
                            update.add_field(actual_column_name, HourTransform(), f"{actual_column_name}_hour")
                        elif transform_name == 'bucket':
                            if len(args) < 2:
                                raise ValueError(f"bucket transform requires 2 arguments: bucket(column, N)")
                            num_buckets = int(args[1])
                            logger.info(f"Adding bucket partition on {actual_column_name} with {num_buckets} buckets")
                            update.add_field(actual_column_name, BucketTransform(num_buckets), f"{actual_column_name}_bucket")
                        elif transform_name == 'truncate':
                            if len(args) < 2:
                                raise ValueError(f"truncate transform requires 2 arguments: truncate(column, width)")
                            width = int(args[1])
                            logger.info(f"Adding truncate partition on {actual_column_name} with width {width}")
                            update.add_field(actual_column_name, TruncateTransform(width), f"{actual_column_name}_trunc")
                        else:
                            raise ValueError(f"Unsupported partition transform: {transform_name}")
                    else:
                        # Identity partition: just column name
                        # Validate column exists
                        schema_fields = {field.name.lower(): field.name for field in table.schema().fields}
                        if spec.lower() not in schema_fields:
                            available_cols = ', '.join(schema_fields.values())
                            raise ValueError(
                                f"Column '{spec}' not found in table schema. "
                                f"Available columns: {available_cols}"
                            )
                        actual_column_name = schema_fields[spec.lower()]
                        logger.info(f"Adding identity partition on {actual_column_name}")
                        update.add_field(actual_column_name, IdentityTransform(), actual_column_name)
            
            logger.info(f"Successfully updated partition spec for {table_identifier}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update partition spec for {table_identifier}: {e}")
            logger.error(f"Partition specs provided: {partition_specs}")
            raise


def evolve_iceberg_schema(
    catalog_config: Dict[str, Any],
    table_identifier: str,
    add_columns: Optional[List[Dict[str, str]]] = None,
    drop_columns: Optional[List[str]] = None
) -> bool:
    """
    Main function to evolve Iceberg table schema
    
    Args:
        catalog_config: PyIceberg catalog configuration
        table_identifier: Full table identifier (namespace.table_name)
        add_columns: Columns to add [{'name': 'col', 'type': 'VARCHAR'}]
        drop_columns: MUST BE None - column drops are not supported
    
    Returns:
        True if all operations successful
    
    Raises:
        Exception if schema evolution fails or if drop_columns is provided
    """
    evolver = IcebergSchemaEvolution(catalog_config)
    
    # Columns are never dropped from target tables
    if drop_columns:
        raise ValueError(
            f"Column drops are not supported. Columns to drop: {drop_columns}. "
            f"Columns removed from source are preserved in target with NULL values."
        )
    
    if add_columns:
        logger.info(f"Adding {len(add_columns)} columns to {table_identifier}")
        evolver.add_columns(table_identifier, add_columns)
    
    return True


def update_iceberg_partitioning(
    catalog_config: Dict[str, Any],
    table_identifier: str,
    partition_specs: Union[str, List[str]]
) -> bool:
    """
    Update partitioning for an Iceberg table
    
    Args:
        catalog_config: PyIceberg catalog configuration
        table_identifier: Full table identifier (namespace.table_name)
        partition_specs: Partition specification(s)
            - Identity: 'country' or ['country', 'region']
            - Day transform: 'day(order_date)' or ['day(order_date)', 'country']
            - Month transform: 'month(order_date)'
            - Year transform: 'year(order_date)'
            - Hour transform: 'hour(timestamp_col)'
            - Unsupported: bucket, truncate (will fail with AWS API error)
    
    Returns:
        True if successful
    
    Raises:
        Exception if partition update fails
    """
    evolver = IcebergSchemaEvolution(catalog_config)
    
    # Convert single string to list
    if isinstance(partition_specs, str):
        partition_specs = [partition_specs]
    
    logger.info(f"Updating partition spec for {table_identifier} with {len(partition_specs)} partition(s)")
    evolver.update_partition_spec(table_identifier, partition_specs)
    
    return True


def pyiceberg_incremental_write(
    catalog_config: Dict[str, Any],
    table_identifier: str,
    new_data_query: str,
    unique_key: Union[str, List[str]],
    duckdb_connection
) -> Dict[str, int]:
    """
    Perform incremental write using PyIceberg UPSERT
    
    This is used for partitioned tables where DuckDB's INSERT/DELETE don't work.
    PyIceberg's upsert method handles row-level updates efficiently, even across partitions.
    
    Note: The table must be created with identifier_field_ids in the schema,
    which maps to the unique_key configuration.
    
    Args:
        catalog_config: PyIceberg catalog configuration
        table_identifier: Full table identifier (namespace.table_name)
        new_data_query: SQL query to get new data from DuckDB
        unique_key: Column(s) to use for upsert matching
        duckdb_connection: DuckDB connection object
    
    Returns:
        Dict with 'rows_updated' and 'rows_inserted' counts
    
    Raises:
        Exception if write fails
    """
    try:
        from pyiceberg.catalog import load_catalog
        
        logger.info(f"Starting PyIceberg upsert for {table_identifier}")
        
        # Step 1: Load table
        catalog = load_catalog('s3_tables', **catalog_config)
        table = catalog.load_table(table_identifier)
        
        # Step 2: Read new data from DuckDB into PyArrow
        # The connection is a DuckDBConnectionWrapper, we need to get the cursor
        logger.info(f"Reading new data from DuckDB: {new_data_query}")
        cursor = duckdb_connection.cursor()
        result = cursor.execute(new_data_query)
        
        # Convert to PyArrow Table (arrow() returns a RecordBatchReader)
        arrow_reader = result.arrow()
        new_data_arrow = arrow_reader.read_all()
        logger.info(f"Read {new_data_arrow.num_rows} rows from DuckDB")
        
        if new_data_arrow.num_rows == 0:
            logger.info("No new data to write")
            return {'rows_updated': 0, 'rows_inserted': 0}
        
        # Step 3: Validate unique_key columns exist in data (case-insensitive)
        if isinstance(unique_key, str):
            unique_key_cols = [unique_key]
        else:
            unique_key_cols = unique_key
        
        # Create a case-insensitive mapping of column names
        column_name_map = {col.lower(): col for col in new_data_arrow.column_names}
        
        # Validate and get actual column names
        actual_unique_key_cols = []
        for col in unique_key_cols:
            col_lower = col.lower()
            if col_lower not in column_name_map:
                available_cols = ', '.join(new_data_arrow.column_names)
                raise ValueError(
                    f"Unique key column '{col}' not found in new data. "
                    f"Available columns: {available_cols}"
                )
            actual_unique_key_cols.append(column_name_map[col_lower])
        
        # Step 4: Set identifier fields if not already set
        # PyIceberg upsert requires identifier_field_ids in the table schema
        schema = table.schema()
        
        # Check if identifier fields are already set
        if not schema.identifier_field_ids:
            logger.info("Table does not have identifier_field_ids set, updating schema...")
            
            # Get field IDs for unique key columns
            # We need to use the actual column names from the table schema (case-sensitive)
            identifier_field_ids = []
            schema_column_names = []
            
            for col_name in actual_unique_key_cols:
                # Find the field in the schema (case-insensitive search)
                field = None
                for schema_field in schema.fields:
                    if schema_field.name.lower() == col_name.lower():
                        field = schema_field
                        break
                
                if field:
                    identifier_field_ids.append(field.field_id)
                    schema_column_names.append(field.name)  # Use actual schema column name
                else:
                    available_cols = ', '.join([f.name for f in schema.fields])
                    raise ValueError(
                        f"Column '{col_name}' not found in table schema. "
                        f"Available columns: {available_cols}"
                    )
            
            logger.info(f"Setting identifier field IDs: {identifier_field_ids} for columns: {schema_column_names}")
            
            # Update the schema to set identifier fields using actual schema column names
            with table.update_schema() as update:
                update.set_identifier_fields(*schema_column_names)
            
            # Reload table to get updated schema
            table = catalog.load_table(table_identifier)
            logger.info("Identifier fields set successfully")
        else:
            logger.info(f"Table already has identifier_field_ids: {schema.identifier_field_ids}")
        
        # Step 5: Perform upsert
        # PyIceberg's upsert method automatically:
        # - Updates existing rows based on identifier_field_ids (unique_key)
        # - Inserts new rows that don't match existing identifiers
        # - Handles cross-partition lookups efficiently
        logger.info(f"Performing upsert with unique_key: {actual_unique_key_cols}")
        result = table.upsert(new_data_arrow)
        
        # Extract results
        rows_updated = getattr(result, 'rows_updated', 0)
        rows_inserted = getattr(result, 'rows_inserted', 0)
        
        logger.info(f"Upsert complete: {rows_updated} updated, {rows_inserted} inserted")
        
        return {
            'rows_updated': rows_updated,
            'rows_inserted': rows_inserted
        }
        
    except Exception as e:
        logger.error(f"Failed to perform PyIceberg upsert: {e}")
        raise
