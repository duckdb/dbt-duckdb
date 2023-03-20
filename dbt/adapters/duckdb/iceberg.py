import pyiceberg as pi


class Iceberg:
    def __init__(self, catalog: pi.catalog.Catalog):
        self._catalog = catalog

    def load(self, database: str, table: str):
        tbl = self._catalog.load_table((database, table))
        # TODO: how to config filters/column selectors, etc.
        return tbl.scan().to_arrow()


def create(name: str, config: dict) -> Iceberg:
    return pi.catalog.load_catalog(name, **config)
