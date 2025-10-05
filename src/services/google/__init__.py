from .bigquery import BigQueryClient

class Google:
    """
    Singleton class for Google Cloud services.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.bigquery = BigQueryClient()
        return cls._instance

    def __init__(self):
        self.bigquery: BigQueryClient = self._instance.bigquery
