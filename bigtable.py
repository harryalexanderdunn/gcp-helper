from google.cloud import bigtable
from google.cloud.bigtable import column_family
from typing import List, Dict, Any

class BigtableClientHelper:
    def __init__(self, project_id: str):
        """
        Initialize the Bigtable client with Project.

        Args:
            project_id (str): The Google Cloud project ID.
            instance_id (str): The Bigtable instance ID.

        Returns:
            None
        """
        self.client = bigtable.Client(project=project_id, admin=True)

    def create_instance(self, instance_id: str, cluster_id: str, location_id: str, serve_nodes: int) -> bigtable.Instance:
        """
        Create a new Bigtable instance within Initialised Project.

        Args:
            instance_id (str): The ID of the instance to create.
            cluster_id (str): The ID of the cluster to create.
            location_id (str): The location (region) of the cluster.
            serve_nodes (int): The number of nodes in the cluster.

        Returns:
            bigtable.Instance: The created instance object.
        """
        instance = self.client.instance(instance_id)
        cluster = instance.cluster(cluster_id, location_id=location_id, serve_nodes=serve_nodes)
        instance.create(clusters=[cluster])
        return instance

    def delete_instance(self, instance_id: str) -> None:
        """
        Delete a Bigtable instance in Initialised Project.

        Args:
            instance_id (str): The ID of the instance to delete.

        Returns:
            None
        """
        instance = self.client.instance(instance_id)
        instance.delete()

    def create_table(self, instance_id: str, table_id: str, column_family_id: str) -> bigtable.Table:
        """
        Create a new table inside specified Bigtable instance.

        Args:
            instance_id (str): The Bigtable instance ID.
            table_id (str): The ID of the table to create.
            column_family_id (str): The ID of the column family to create.

        Returns:
            bigtable.Table: The created table object.
        """
        instance = self.client.instance(instance_id)
        table = instance.table(table_id)
        column_family = table.column_family(column_family_id)
        table.create(column_families={column_family_id: column_family})
        return table

    def delete_table(self, instance_id: str, table_id: str) -> None:
        """
        Delete a table inside a specified Bigtable instance.

        Args:
            instance_id (str): The Bigtable instance ID.
            table_id (str): The ID of the table to delete.

        Returns:
            None
        """
        instance = self.client.instance(instance_id)
        table = instance.table(table_id)
        table.delete()

    def write_row(self, instance_id: str, table_id: str, row_key: str, column_family_id: str, column: str, value: str) -> None:
        """
        Write a row to a table in a specified Bigtable instance.

        Args:
            instance_id (str): The Bigtable instance ID.
            table_id (str): The ID of the table.
            row_key (str): The key of the row to write.
            column_family_id (str): The ID of the column family.
            column (str): The column name.
            value (str): The value to write.

        Returns:
            None
        """
        instance = self.client.instance(instance_id)
        table = instance.table(table_id)
        row = table.direct_row(row_key)
        row.set_cell(column_family_id, column, value)
        row.commit()

    def read_row(self, instance_id: str, table_id: str, row_key: str) -> Dict[str, Any]:
        """
        Read a row from a table in the specified Bigtable instance.

        Args:
            instance_id (str): The Bigtable instance ID.
            table_id (str): The ID of the table.
            row_key (str): The key of the row to read.

        Returns:
            Dict[str, Any]: The row data.
        """
        instance = self.client.instance(instance_id)
        table = instance.table(table_id)
        row = table.read_row(row_key)
        return row.to_dict() if row else {}

    def list_tables(self, instance_id: str) -> List[str]:
        """
        List all tables in the specified Bigtable instance.

        Args:
            instance_id (str): The Bigtable instance ID.

        Returns:
            List[str]: A list of table IDs.
        """
        instance = self.client.instance(instance_id)
        tables = instance.list_tables()
        return [table.table_id for table in tables]