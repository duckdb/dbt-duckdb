import deltalake as dl


class Delta:
    def __init__(self, catalog: dl.DataCatalog):
        self._catalog = catalog

    def load(self, database: str, table: str):
        dt = dl.DeltaTable.from_data_catalog(
            data_catalog=self._catalog, database_name=database, table_name=table
        )
        return dt.to_pyarrow_dataset()


def create(name: str, config: dict):
    # Note: there is currenly only one supported catalog impl for deltalake
    # and it takes no parameters
    return Delta(dl.DataCatalog.AWS)
