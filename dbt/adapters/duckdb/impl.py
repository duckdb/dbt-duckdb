import os
import shutil
from typing import List

from dbt.adapters.duckdb import DuckDBConnectionManager
from dbt.adapters.duckdb import DuckDBRelation
from dbt.adapters.duckdb import util
from dbt.adapters.duckdb.relation import DuckDBRelationType

from dbt.adapters.sql import SQLAdapter


class DuckDBAdapter(SQLAdapter):
    ConnectionManager = DuckDBConnectionManager
    Relation = DuckDBRelation

    @classmethod
    def date_function(cls):
        return "now()"

    @classmethod
    def is_cancelable(cls):
        return False
    
    def get_columns_in_relation(self, relation):
        return super().get_columns_in_relation(relation)

    def drop_relation(self, relation):
        if util.is_filepath(relation.database):
            path = os.path.join(relation.database, relation.schema, relation.identifier)
            if relation.is_parquet():
                path += ".parquet"
            elif relation.is_csv():
                path += ".csv"
            if os.path.exists(path):
                os.remove(path)
        else:
            return super().drop_relation(relation)

    def create_schema(self, relation: DuckDBRelation) -> None:
        if util.is_filepath(relation.database):
            path = os.path.join(relation.database, relation.schema)
            if not os.path.exists(path):
                os.mkdir(path)
        else:
            super().create_schema(relation)

    def drop_schema(self, relation: DuckDBRelation) -> None:
        if util.is_filepath(relation.database):
            path = os.path.join(relation.database, relation.schema)
            if os.path.exists(path):
                shutil.rmtree(path)
            self.cache.drop_schema(relation.database, relation.schema)
        else:
            super().drop_schema(relation)

    def list_relations_without_caching(
        self,
        schema_relation: DuckDBRelation,
    ) -> List[DuckDBRelation]:
        if util.is_filepath(schema_relation.database):
            path = os.path.join(schema_relation.database, schema_relation.schema)
            if not os.path.exists(path):
                return []
            contents = os.listdir(path)
            relations = []
            for c in contents:
                if os.path.isfile(os.path.join(path, c)):
                    name, ext = os.path.splitext(c)
                    relation_type = None
                    if ext == ".parquet":
                        relation_type = DuckDBRelationType.Parquet
                    elif ext == ".csv":
                        relation_type = DuckDBRelationType.CSV
                    if relation_type:
                        relations.append(
                            DuckDBRelation.create(
                                database=schema_relation.database,
                                schema=schema_relation.schema,
                                identifier=name,
                                type=relation_type,
                            )
                        ) 
            return relations
        else:
            return super().list_relations_without_caching(schema_relation)

    def list_schemas(self, database: str) -> List[str]:
        if util.is_filepath(database):
            database = database.strip("'")
            contents = os.listdir(database)
            return [c for c in contents if os.path.isdir(os.path.join(database, c))]
        else:
            return super().list_schemas(database)

    def check_schema_exists(self, database: str, schema: str) -> bool:
        if util.util.is_filepath(database):
            return os.path.exists(os.path.join(database, schema))
        else:
            return super().check_schema_exists(database, schema)