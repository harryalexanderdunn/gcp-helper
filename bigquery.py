from google.cloud import bigquery

class BigQueryClientHelper:
    def __init__(self, project_id: str):
        """
        Initializes the BigQuery client within specified project.
        
        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.project_id = project_id
        self.client = bigquery.Client(project_id=project_id)
    
    def create_dataset(self, dataset_id: str) -> object:
        """
        Create a new dataset in BigQuery in initialised project.

        Args:
            dataset_id (str): The ID of the dataset to create.

        Returns:
            object: dataset object
        """
        dataset = bigquery.Dataset(f"{self.client.project}.{dataset_id}")
        return self.client.create_dataset(dataset, exists_ok=True)

    def delete_dataset(self, dataset_id: str, delete_contents: bool = False) -> None:
        """
        Delete a dataset in BigQuery within initialised project.

        Args:
            dataset_id (str): The ID of the dataset to delete.
            delete_contents (bool): Whether to delete all the contents in the dataset.

        Returns:
            None
        """
        self.client.delete_dataset(dataset_id, delete_contents=delete_contents, not_found_ok=True)

    def query_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query in initialised project and return the results as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame: A DataFrame containing the query results.
        """
        query_job = self.client.query(query)
        return query_job.to_dataframe()

    def insert_data(self, dataset_name: str, table_name: str, rows: List[Dict[str, Any]]) -> None:
        """
        Insert rows into a BigQuery table that already exists within initialised project.

        Args:
            dataset_name (str): The name of the dataset where the table sits.
            table_name (str): The name of the table to insert data into.
            rows (List[Dict[str, Any]]): A list of dictionaries representing the rows to insert.

        Returns:
            None
        """
        table_id = f"{self.project_id}.{dataset_name}.{table_name}"
        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            raise Exception(f"Failed to insert rows: {errors}")

    def create_table(self,
                    dataset_name: str,
                    table_name: str, 
                    schema: List[bigquery.SchemaField], 
                    partition_field: str = None, 
                    cluster_fields: List[str] = None) -> object:
        """
        Create a new table in BigQuery in initialised project with specified schema and partion/cluster fields (optional).

        Args:
            dataset_name (str): name of the dataset where the table is being created
            table_name (str): The name of the table to create.
            schema (List[bigquery.SchemaField]): The schema of the table.
            partition_field (str, optional): The field to partition the table by.
            cluster_fields (List[str], optional): The fields to cluster the table by.

        Returns:
            object: Table object
        """
        table_id = f"{self.project_id}.{dataset_name}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(field=partition_field)
        if cluster_fields:
            table.clustering_fields = cluster_fields
        return self.client.create_table(table, exists_ok=True)

    def delete_table(self, dataset_name: str, table_name: str) -> None:
        """
        Delete a table in BigQuery in initialised project.

        Args:
            dataset_name (str): The name of the dataset where the table sits.
            table_name (str): The name of the table to delete.

        Returns:
            None
        """
        self.client.delete_table(f"{self.project_id}.{dataset_name}.{table_name}", not_found_ok=True)

    def create_view(self, dataset_name: str, view_name: str, query: str) -> None:
        """
        Create a new view in BigQuery in initialised project.

        Args:
            dataset_name (str): name of the dataset where the table is being created
            view_name (str): The table name of the view to create.
            query (str): The SQL query that defines the view.

        Returns:
            None
        """
        view_id = f"{self.project_id}.{dataset_name}.{view_name}"
        view = bigquery.Table(view_id)
        view.view_query = query
        self.client.create_table(view, exists_ok=True)

    def delete_view(self, dataset_name: str, view_name: str) -> None:
        """
        Delete a view in BigQuery in initialised project.

        Args:
            dataset_name (str): name of the dataset where the table is being created
            view_name (str): The name of the view to delete.

        Returns:
            None
        """
        view_id = f"{self.project_id}.{dataset_name}.{view_name}"
        self.client.delete_table(view_id, not_found_ok=True)
    