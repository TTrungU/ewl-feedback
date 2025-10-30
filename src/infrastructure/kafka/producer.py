
import json
import asyncio
import logging
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from ...core.entities.feedback import Feedback

logger = logging.getLogger(__name__)


class KafkaFeedbackProducer:
    """Kafka producer for publishing feedback events"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        max_retry_attempts: int = 3,
        retry_backoff_ms: int = 1000
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.max_retry_attempts = max_retry_attempts
        self.retry_backoff_ms = retry_backoff_ms
        self.producer = None
        self.started = False
        
    async def start(self):
        """Start the Kafka producer"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=self._serialize_message,
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retry_backoff_ms=self.retry_backoff_ms,
                request_timeout_ms=30000,  # keep this if you want timeout
                linger_ms=5,               # safe to use
                acks="all",                # safe to use
                compression_type="gzip"    # safe to use
            )
            
            await self.producer.start()
            self.started = True
            logger.info(f"Kafka producer started for topic: {self.topic}")
            
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {str(e)}")
            raise KafkaProducerError(f"Failed to start producer: {str(e)}")
    
    async def stop(self):
        """Stop the Kafka producer"""
        try:
            if self.producer:
                await self.producer.stop()
                self.started = False
                logger.info("Kafka producer stopped")
        except Exception as e:
            logger.error(f"Error stopping Kafka producer: {str(e)}")
    
    async def publish_feedback(self, feedback: Feedback, key: Optional[str] = None) -> bool:
        """
        Publish feedback to Kafka topic
        
        Args:
            feedback: Feedback entity to publish
            key: Optional partition key (defaults to user_id)
            
        Returns:
            bool: True if published successfully
        """
        if not self.started:
            raise KafkaProducerError("Producer not started")
        
        # Use user_id as default key for partitioning
        if key is None:
            key = feedback.user_id
        
        message = self._create_feedback_message(feedback)
        
        for attempt in range(self.max_retry_attempts):
            try:
                # Send message
                record_metadata = await self.producer.send_and_wait(
                    self.topic,
                    value=message,
                    key=key
                )
                
                logger.info(
                    f"Published feedback {feedback.id} to partition {record_metadata.partition} "
                    f"at offset {record_metadata.offset}"
                )
                return True
                
            except KafkaError as e:
                logger.warning(f"Kafka error on attempt {attempt + 1}: {str(e)}")
                if attempt < self.max_retry_attempts - 1:
                    await asyncio.sleep(self.retry_backoff_ms / 1000.0)
                else:
                    logger.error(f"Failed to publish feedback {feedback.id} after {self.max_retry_attempts} attempts")
                    raise KafkaProducerError(f"Failed to publish after retries: {str(e)}")
            
            except Exception as e:
                logger.error(f"Unexpected error publishing feedback {feedback.id}: {str(e)}")
                raise KafkaProducerError(f"Unexpected error: {str(e)}")
        
        return False
    
    async def publish_feedback_event(self, event_type: str, feedback: Feedback, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Publish feedback event with additional metadata
        
        Args:
            event_type: Type of event (e.g., 'feedback_generated', 'feedback_updated')
            feedback: Feedback entity
            metadata: Additional event metadata
            
        Returns:
            bool: True if published successfully
        """
        if not self.started:
            raise KafkaProducerError("Producer not started")
        
        message = self._create_feedback_event_message(event_type, feedback, metadata)
        
        try:
            record_metadata = await self.producer.send_and_wait(
                self.topic,
                value=message,
                key=feedback.user_id
            )
            
            logger.info(
                f"Published {event_type} event for feedback {feedback.id} "
                f"to partition {record_metadata.partition} at offset {record_metadata.offset}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish {event_type} event: {str(e)}")
            raise KafkaProducerError(f"Failed to publish event: {str(e)}")
    
    async def publish_batch_feedback(self, feedbacks: list[Feedback]) -> Dict[str, bool]:
        """
        Publish multiple feedback messages in batch
        
        Args:
            feedbacks: List of feedback entities
            
        Returns:
            Dict[str, bool]: Results keyed by feedback ID
        """
        if not self.started:
            raise KafkaProducerError("Producer not started")
        
        results = {}
        
        # Create futures for all messages
        futures = []
        for feedback in feedbacks:
            message = self._create_feedback_message(feedback)
            future = self.producer.send(
                self.topic,
                value=message,
                key=feedback.user_id
            )
            futures.append((feedback.id, future))
        
        # Wait for all futures to complete
        for feedback_id, future in futures:
            try:
                record_metadata = await future
                results[feedback_id] = True
                logger.info(f"Published feedback {feedback_id} to partition {record_metadata.partition}")
            except Exception as e:
                logger.error(f"Failed to publish feedback {feedback_id}: {str(e)}")
                results[feedback_id] = False
        
        return results
    
    def _create_feedback_message(self, feedback: Feedback) -> Dict[str, Any]:
        """Create message from feedback entity"""
        message = {
            "feedback_id": feedback.id,
            "submission_id": feedback.submission_id,
            "user_id": feedback.user_id,
            "generated_at": feedback.generated_at.isoformat(),
            "llm_model": feedback.llm_model,
            "processing_time": feedback.processing_time,
            "metadata": feedback.metadata,
            "raw_llm_output": feedback.raw_llm_output
        }
        
        # Add detailed feedback if present
        if feedback.feedback:
            detailed_feedback = {
                "overall_score": feedback.feedback.overall_score,
                "task_achievement": {
                    "score": feedback.feedback.task_achievement.score,
                    "comments": feedback.feedback.task_achievement.comments,
                    "suggestions": feedback.feedback.task_achievement.suggestions
                },
                "coherence_cohesion": {
                    "score": feedback.feedback.coherence_cohesion.score,
                    "comments": feedback.feedback.coherence_cohesion.comments,
                    "suggestions": feedback.feedback.coherence_cohesion.suggestions
                },
                "lexical_resource": {
                    "score": feedback.feedback.lexical_resource.score,
                    "comments": feedback.feedback.lexical_resource.comments,
                    "suggestions": feedback.feedback.lexical_resource.suggestions
                },
                "grammatical_range_accuracy": {
                    "score": feedback.feedback.grammatical_range_accuracy.score,
                    "comments": feedback.feedback.grammatical_range_accuracy.comments,
                    "suggestions": feedback.feedback.grammatical_range_accuracy.suggestions
                },
                "sentence_feedback": []
            }
            
            # Add sentence-level feedback
            for sent_feedback in feedback.feedback.sentence_feedback:
                sentence_data = {
                    "sentence_id": sent_feedback.sentence_id,
                    "text": sent_feedback.text,
                    "improved_text": sent_feedback.improved_text,
                    "feedback_details": {}
                }
                
                for criteria, detail in sent_feedback.feedback_details.items():
                    sentence_data["feedback_details"][criteria] = {
                        "comments": detail.comments,
                        "suggestions": detail.suggestions
                    }
                
                detailed_feedback["sentence_feedback"].append(sentence_data)
            
            message["feedback"] = detailed_feedback
        
        return message
    
    def _create_feedback_event_message(self, event_type: str, feedback: Feedback, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create event message with additional metadata"""
        message = {
            "event_type": event_type,
            "timestamp": feedback.updated_at.isoformat(),
            "feedback_id": feedback.id,
            "submission_id": feedback.submission_id,
            "user_id": feedback.user_id,
            "status": feedback.status.value,
            "metadata": metadata or {}
        }
        
        # Add feedback data for certain event types
        if event_type in ['feedback_generated', 'feedback_updated']:
            message["feedback"] = {
                "content": feedback.content,
                "score": feedback.score,
                "suggestions": feedback.suggestions,
                "strengths": feedback.strengths,
                "weaknesses": feedback.weaknesses
            }
        
        return message
    
    def _serialize_message(self, message: Dict[str, Any]) -> bytes:
        """Serialize message to bytes"""
        try:
            return json.dumps(message, ensure_ascii=False).encode('utf-8')
        except Exception as e:
            logger.error(f"Failed to serialize message: {str(e)}")
            raise MessageSerializationError(f"Serialization error: {str(e)}")
    
    async def get_producer_info(self) -> Dict[str, Any]:
        """Get producer information"""
        if not self.producer:
            return {"status": "not_started"}
        
        try:
            metadata = await self.producer.client.cluster.metadata()
            
            return {
                "status": "running" if self.started else "stopped",
                "topic": self.topic,
                "bootstrap_servers": self.bootstrap_servers,
                "retry_attempts": self.max_retry_attempts,
                "retry_backoff_ms": self.retry_backoff_ms
            }
            
        except Exception as e:
            logger.error(f"Error getting producer info: {str(e)}")
            return {"status": "error", "error": str(e)}


class KafkaProducerManager:
    """Manager for multiple Kafka producers"""
    
    def __init__(self):
        self.producers: Dict[str, KafkaFeedbackProducer] = {}
    
    def add_producer(self, name: str, producer: KafkaFeedbackProducer):
        """Add a producer to the manager"""
        self.producers[name] = producer
    
    async def start_all(self):
        """Start all producers"""
        for name, producer in self.producers.items():
            try:
                await producer.start()
                logger.info(f"Started producer: {name}")
            except Exception as e:
                logger.error(f"Failed to start producer {name}: {str(e)}")
                raise
    
    async def stop_all(self):
        """Stop all producers"""
        for name, producer in self.producers.items():
            try:
                await producer.stop()
                logger.info(f"Stopped producer: {name}")
            except Exception as e:
                logger.error(f"Error stopping producer {name}: {str(e)}")
    
    async def get_status(self) -> Dict[str, Any]:
        """Get status of all producers"""
        status = {}
        for name, producer in self.producers.items():
            status[name] = await producer.get_producer_info()
        return status


class KafkaProducerError(Exception):
    """Exception raised when Kafka producer operations fail"""
    pass


class MessageSerializationError(KafkaProducerError):
    """Exception raised when message serialization fails"""
    pass