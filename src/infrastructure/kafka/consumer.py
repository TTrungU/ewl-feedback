import logging
import json
import asyncio
from typing import Dict, Any, Callable, Optional
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from ...core.entities.feedback import Submission, SubmissionType

logger = logging.getLogger(__name__)


class KafkaSubmissionConsumer:
    """Kafka consumer for processing test submissions"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        message_handler: Callable[[Submission], None],
        max_poll_interval_ms: int = 300000,
        session_timeout_ms: int = 30000
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.message_handler = message_handler
        self.consumer = None
        self.running = False
        self.max_poll_interval_ms = max_poll_interval_ms
        self.session_timeout_ms = session_timeout_ms
        
    async def start(self):
        """Start the Kafka consumer"""
        try:
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                max_poll_interval_ms=self.max_poll_interval_ms,
                session_timeout_ms=self.session_timeout_ms,
                value_deserializer=self._deserialize_message
            )
            
            await self.consumer.start()
            self.running = True
            logger.info(f"Kafka consumer started for topic: {self.topic}")
            
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {str(e)}")
            raise KafkaConsumerError(f"Failed to start consumer: {str(e)}")
    
    async def stop(self):
        """Stop the Kafka consumer"""
        try:
            self.running = False
            if self.consumer:
                await self.consumer.stop()
                logger.info("Kafka consumer stopped")
        except Exception as e:
            logger.error(f"Error stopping Kafka consumer: {str(e)}")
    
    async def consume_messages(self):
        """Consume messages from Kafka topic"""
        if not self.running:
            raise KafkaConsumerError("Consumer not started")
        
        try:
            async for msg in self.consumer:
                try:
                    submission = self._parse_submission(msg.value)
                    logger.info(f"Received submission: {submission.id}")
                    
                    # Process the submission asynchronously
                    await self._process_submission(submission)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    # Continue processing other messages
                    continue
                    
        except KafkaError as e:
            logger.error(f"Kafka error: {str(e)}")
            raise KafkaConsumerError(f"Kafka error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in message consumption: {str(e)}")
            raise KafkaConsumerError(f"Unexpected error: {str(e)}")
    
    async def _process_submission(self, submission: Submission):
        """Process a single submission"""
        try:
            # Call the message handler
            if asyncio.iscoroutinefunction(self.message_handler):
                await self.message_handler(submission)
            else:
                self.message_handler(submission)
                
        except Exception as e:
            logger.error(f"Error in message handler for submission {submission.id}: {str(e)}")
            raise
    
    def _deserialize_message(self, message: bytes) -> Dict[str, Any]:
        """Deserialize Kafka message"""
        try:
            return json.loads(message.decode('utf-8'))
        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize message: {str(e)}")
            raise MessageDeserializationError(f"Invalid JSON: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error deserializing message: {str(e)}")
            raise MessageDeserializationError(f"Deserialization error: {str(e)}")
    
    def _parse_submission(self, data: Dict[str, Any]) -> Submission:
        """Parse message data into Submission entity"""
        try:
            # Handle different message formats
            if 'submission' in data:
                submission_data = data['submission']
            else:
                submission_data = data
            
            return Submission(
                id=submission_data.get('id', ''),
                user_id=submission_data.get('user_id', ''),
                content=submission_data.get('content', ''),
                submission_type=SubmissionType(submission_data.get('submission_type', 'code')),
                metadata=submission_data.get('metadata', {}),
                created_at=submission_data.get('created_at')
            )
            
        except KeyError as e:
            logger.error(f"Missing required field in submission: {str(e)}")
            raise MessageParsingError(f"Missing required field: {str(e)}")
        except ValueError as e:
            logger.error(f"Invalid submission type: {str(e)}")
            raise MessageParsingError(f"Invalid submission type: {str(e)}")
        except Exception as e:
            logger.error(f"Error parsing submission: {str(e)}")
            raise MessageParsingError(f"Parsing error: {str(e)}")
    
    async def get_consumer_info(self) -> Dict[str, Any]:
        """Get consumer information"""
        if not self.consumer:
            return {"status": "not_started"}
        
        try:
            # Get partition assignments
            partitions = self.consumer.assignment()
            
            # Get consumer group metadata
            metadata = await self.consumer.client.cluster.metadata()
            
            return {
                "status": "running" if self.running else "stopped",
                "topic": self.topic,
                "group_id": self.group_id,
                "partitions": len(partitions),
                "bootstrap_servers": self.bootstrap_servers
            }
            
        except Exception as e:
            logger.error(f"Error getting consumer info: {str(e)}")
            return {"status": "error", "error": str(e)}


class KafkaConsumerManager:
    """Manager for multiple Kafka consumers"""
    
    def __init__(self):
        self.consumers: Dict[str, KafkaSubmissionConsumer] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
    
    def add_consumer(self, name: str, consumer: KafkaSubmissionConsumer):
        """Add a consumer to the manager"""
        self.consumers[name] = consumer
    
    async def start_all(self):
        """Start all consumers"""
        for name, consumer in self.consumers.items():
            await self._start_consumer(name, consumer)
    
    async def _start_consumer(self, name: str, consumer: KafkaSubmissionConsumer):
        """Start a single consumer"""
        try:
            await consumer.start()
            
            # Create task for consuming messages
            task = asyncio.create_task(consumer.consume_messages())
            self.tasks[name] = task
            
            logger.info(f"Started consumer: {name}")
            
        except Exception as e:
            logger.error(f"Failed to start consumer {name}: {str(e)}")
            raise
    
    async def stop_all(self):
        """Stop all consumers"""
        # Cancel all tasks
        for name, task in self.tasks.items():
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        
        # Stop all consumers
        for name, consumer in self.consumers.items():
            try:
                await consumer.stop()
                logger.info(f"Stopped consumer: {name}")
            except Exception as e:
                logger.error(f"Error stopping consumer {name}: {str(e)}")
        
        self.tasks.clear()
    
    async def get_status(self) -> Dict[str, Any]:
        """Get status of all consumers"""
        status = {}
        for name, consumer in self.consumers.items():
            status[name] = await consumer.get_consumer_info()
        return status


class KafkaConsumerError(Exception):
    """Exception raised when Kafka consumer operations fail"""
    pass


class MessageDeserializationError(KafkaConsumerError):
    """Exception raised when message deserialization fails"""
    pass


class MessageParsingError(KafkaConsumerError):
    """Exception raised when message parsing fails"""
    pass