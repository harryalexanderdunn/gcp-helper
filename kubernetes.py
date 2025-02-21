from kubernetes import client, config
from typing import List, Dict, Any

class KubernetesClientHelper:
    def __init__(self, kubeconfig_path: str = None):
        """
        Initialize the Kubernetes client.

        Args:
            kubeconfig_path (str, optional): The path to the kubeconfig file. If None, it uses the default kubeconfig.

        Returns:
            None
        """
        if kubeconfig_path:
            config.load_kube_config(config_file=kubeconfig_path)
        else:
            config.load_kube_config()
        self.v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()

    def list_pods(self, namespace: str = 'default') -> List[Dict[str, Any]]:
        """
        List all pods in a namespace.

        Args:
            namespace (str): The namespace to list pods from.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the pods.
        """
        pods = self.v1.list_namespaced_pod(namespace)
        return [pod.to_dict() for pod in pods.items]

    def create_pod(self, namespace: str, pod_manifest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new pod in a namespace.

        Args:
            namespace (str): The namespace to create the pod in.
            pod_manifest (Dict[str, Any]): The manifest of the pod to create.

        Returns:
            Dict[str, Any]: The created pod object.
        """
        pod = self.v1.create_namespaced_pod(namespace, pod_manifest)
        return pod.to_dict()

    def delete_pod(self, namespace: str, pod_name: str) -> None:
        """
        Delete a pod in a namespace.

        Args:
            namespace (str): The namespace of the pod.
            pod_name (str): The name of the pod to delete.

        Returns:
            None
        """
        self.v1.delete_namespaced_pod(pod_name, namespace)

    def list_nodes(self) -> List[Dict[str, Any]]:
        """
        List all nodes in the cluster.

        Args:
            None

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the nodes.
        """
        nodes = self.v1.list_node()
        return [node.to_dict() for node in nodes.items]

    def get_node(self, node_name: str) -> Dict[str, Any]:
        """
        Get details of a specific node.

        Args:
            node_name (str): The name of the node.

        Returns:
            Dict[str, Any]: The node object.
        """
        node = self.v1.read_node(node_name)
        return node.to_dict()

    def create_deployment(self, namespace: str, deployment_manifest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new deployment in a namespace.

        Args:
            namespace (str): The namespace to create the deployment in.
            deployment_manifest (Dict[str, Any]): The manifest of the deployment to create.

        Returns:
            Dict[str, Any]: The created deployment object.
        """
        deployment = self.apps_v1.create_namespaced_deployment(namespace, deployment_manifest)
        return deployment.to_dict()

    def delete_deployment(self, namespace: str, deployment_name: str) -> None:
        """
        Delete a deployment in a namespace.

        Args:
            namespace (str): The namespace of the deployment.
            deployment_name (str): The name of the deployment to delete.

        Returns:
            None
        """
        self.apps_v1.delete_namespaced_deployment(deployment_name, namespace)