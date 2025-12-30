"""
Iceberg schema evolution utilities using PyIceberg for AWS S3 Tables
"""
from typing import List, Dict, Any, Optional
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
