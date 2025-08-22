import os
from typing import List
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Application settings"""
    
    # API Settings
    api_host: str = Field(default="0.0.0.0", env="API_HOST")
    api_port: int = Field(default=8000, env="API_PORT")
    debug: bool = Field(default=False, env="DEBUG")
    cors_origins: List[str] = Field(default=["*"], env="CORS_ORIGINS")
    
    # MongoDB Settings
    mongodb_url: str = Field(..., env="MONGODB_URL")
    mongodb_database: str = Field(default="feedback_service", env="MONGODB_DATABASE")
    
    # Kafka Settings
    kafka_bootstrap_servers: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    kafka_submission_topic: str = Field(default="test_submissions", env="KAFKA_SUBMISSION_TOPIC")
    kafka_feedback_topic: str = Field(default="feedback_results", env="KAFKA_FEEDBACK_TOPIC")
    kafka_consumer_group: str = Field(default="feedback_service_group", env="KAFKA_CONSUMER_GROUP")
    
    # LLM API Settings
    llm_api_url: str = Field(..., env="LLM_API_URL")
    llm_api_key: str = Field(..., env="LLM_API_KEY")
    llm_timeout: int = Field(default=30, env="LLM_TIMEOUT")
    llm_model: str = Field(default="gpt-3.5-turbo", env="LLM_MODEL")
    
    # Service Settings
    max_retry_attempts: int = Field(default=3, env="MAX_RETRY_ATTEMPTS")
    retry_backoff_ms: int = Field(default=1000, env="RETRY_BACKOFF_MS")
    
    # Logging Settings
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        env="LOG_FORMAT"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        
    def get_mongodb_connection_params(self) -> dict:
        """Get MongoDB connection parameters"""
        return {
            "host": self.mongodb_url,
            "maxPoolSize": 10,
            "minPoolSize": 1,
            "maxIdleTimeMS": 30000,
            "connectTimeoutMS": 20000,
            "serverSelectionTimeoutMS": 20000
        }
    
    def get_kafka_consumer_config(self) -> dict:
        """Get Kafka consumer configuration"""
        return {
            "bootstrap_servers": self.kafka_bootstrap_servers,
            "group_id": self.kafka_consumer_group,
            "auto_offset_reset": "earliest",
            "enable_auto_commit": True,
            "auto_commit_interval_ms": 5000,
            "max_poll_interval_ms": 300000,
            "session_timeout_ms": 30000
        }
    
    def get_kafka_producer_config(self) -> dict:
        """Get Kafka producer configuration"""
        return {
            "bootstrap_servers": self.kafka_bootstrap_servers,
            "acks": "all",
            "retries": self.max_retry_attempts,
            "retry_backoff_ms": self.retry_backoff_ms,
            "enable_idempotence": True,
            "max_in_flight_requests_per_connection": 1
        }