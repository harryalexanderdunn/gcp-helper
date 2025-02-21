from google.cloud import pubsub_v1
from typing import List

class PubSubClientHelper:
    def __init__(self, project_id: str):
        """
        Initialize the Pub/Sub client within Project ID.

        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

    def create_topic(self, topic_id: str) -> pubsub_v1.types.Topic:
        """
        Create a new topic in Pub/Sub within initialised project.

        Args:
            topic_id (str): The ID of the topic to create.

        Returns:
            pubsub_v1.types.Topic: The created topic object.
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        topic = self.publisher.create_topic(request={"name": topic_path})
        return topic

    def delete_topic(self, topic_id: str) -> None:
        """
        Delete a topic in Pub/Sub within initialised project.

        Args:
            topic_id (str): The ID of the topic to delete.

        Returns:
            None
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        self.publisher.delete_topic(request={"topic": topic_path})

    def publish_message(self, topic_id: str, message: str) -> str:
        """
        Publish a message to a topic within initialised project.

        Args:
            topic_id (str): The ID of the topic to publish to.
            message (str): The message to publish.

        Returns:
            str: The message ID of the published message.
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        future = self.publisher.publish(topic_path, message.encode("utf-8"))
        message_id = future.result()
        return message_id

    def create_subscription(self, topic_id: str, subscription_id: str) -> pubsub_v1.types.Subscription:
        """
        Create a new subscription to a topic within initialised project.

        Args:
            topic_id (str): The ID of the topic to subscribe to.
            subscription_id (str): The ID of the subscription to create.

        Returns:
            pubsub_v1.types.Subscription: The created subscription object.
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_id)
        subscription = self.subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
        return subscription

    def delete_subscription(self, subscription_id: str) -> None:
        """
        Delete a subscription in Pub/Sub within initialised project.

        Args:
            subscription_id (str): The ID of the subscription to delete.

        Returns:
            None
        """
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_id)
        self.subscriber.delete_subscription(request={"subscription": subscription_path})

    def pull_messages(self, subscription_id: str, max_messages: int = 10) -> List[str]:
        """
        Pull messages from a subscription within initialised project.

        Args:
            subscription_id (str): The ID of the subscription to pull messages from.
            max_messages (int): The maximum number of messages to pull.

        Returns:
            List[str]: A list of pulled messages.
        """
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_id)
        response = self.subscriber.pull(request={"subscription": subscription_path, "max_messages": max_messages})
        messages = [msg.message.data.decode("utf-8") for msg in response.received_messages]
        ack_ids = [msg.ack_id for msg in response.received_messages]
        self.subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
        return messages