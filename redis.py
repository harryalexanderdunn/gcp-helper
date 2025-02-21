from google.cloud import redis_v1
from google.protobuf.duration_pb2 import Duration
from typing import List, Dict, Any

class MemorystoreClientHelper:
    def __init__(self, project_id: str, location: str):
        """
        Initialize the Memorystore client.

        Args:
            project_id (str): The Google Cloud project ID.
            location (str): The location (region) of the Memorystore instances.

        Returns:
            None
        """
        self.client = redis_v1.CloudRedisClient()
        self.parent = f"projects/{project_id}/locations/{location}"

    def create_instance(self, instance_id: str, tier: str, memory_size_gb: int) -> Dict[str, Any]:
        """
        Create a new Memorystore instance.

        Args:
            instance_id (str): The ID of the instance to create.
            tier (str): The service tier (BASIC or STANDARD_HA).
            memory_size_gb (int): The memory size of the instance in GB.

        Returns:
            Dict[str, Any]: The created instance object.
        """
        instance = {
            "tier": tier,
            "memory_size_gb": memory_size_gb,
        }
        operation = self.client.create_instance(parent=self.parent, instance_id=instance_id, instance=instance)
        return operation.result().to_dict()

    def delete_instance(self, instance_id: str) -> None:
        """
        Delete a Memorystore instance.

        Args:
            instance_id (str): The ID of the instance to delete.

        Returns:
            None
        """
        name = f"{self.parent}/instances/{instance_id}"
        self.client.delete_instance(name=name)

    def get_instance(self, instance_id: str) -> Dict[str, Any]:
        """
        Get details of a specific Memorystore instance.

        Args:
            instance_id (str): The ID of the instance.

        Returns:
            Dict[str, Any]: The instance object.
        """
        name = f"{self.parent}/instances/{instance_id}"
        instance = self.client.get_instance(name=name)
        return instance.to_dict()

    def list_instances(self) -> List[Dict[str, Any]]:
        """
        List all Memorystore instances in the specified location.

        Args:
            None

        Returns:
            List[Dict[str, Any]]: A list of instance objects.
        """
        instances = self.client.list_instances(parent=self.parent)
        return [instance.to_dict() for instance in instances]