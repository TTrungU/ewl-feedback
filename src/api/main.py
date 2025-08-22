import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

from .routes.feedback import router as feedback_router, get_store_feedback_use_case, get_evaluate_submission_use_case
from ..core.use_cases.store_feedback import StoreFeedbackUseCase
from ..core.use_cases.evaluate_submission import EvaluateSubmissionUseCase
from ..infrastructure.mongodb.feedback_repository import MongoFeedbackRepository
from ..infrastructure.llm.http_llm_gateway import HttpLLMGateway
from ..infrastructure.kafka.consumer import KafkaSubmissionConsumer, KafkaConsumerManager
from ..infrastructure.kafka.producer import KafkaFeedbackProducer, KafkaProducerManager
from ..config.settings import Settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global variables for dependency injection
settings = Settings()
mongo_client = None
feedback_repository = None
llm_gateway = None
store_feedback_use_case = None
evaluate_submission_use_case = None
consumer_manager = None
producer_manager = None


async def setup_dependencies():
    """Setup all dependencies"""
    global mongo_client, feedback_repository, llm_gateway
    global store_feedback_use_case, evaluate_submission_use_case
    global consumer_manager, producer_manager
    
    try:
        # Initialize MongoDB
        mongo_client = AsyncIOMotorClient(settings.mongodb_url)
        feedback_repository = MongoFeedbackRepository(mongo_client, settings.mongodb_database)
        await feedback_repository.initialize()
        logger.info("MongoDB initialized successfully")
        
        # Initialize LLM Gateway
        llm_gateway = HttpLLMGateway(
            base_url=settings.llm_api_url,
            api_key=settings.llm_api_key,
            timeout=settings.llm_timeout
        )
        logger.info("LLM Gateway initialized successfully")
        
        # Initialize Use Cases
        store_feedback_use_case = StoreFeedbackUseCase(feedback_repository)
        evaluate_submission_use_case = EvaluateSubmissionUseCase(feedback_repository, llm_gateway)
        logger.info("Use cases initialized successfully")
        
        # Initialize Kafka components
        await setup_kafka()
        
    except Exception as e:
        logger.error(f"Failed to setup dependencies: {str(e)}")
        raise


async def setup_kafka():
    """Setup Kafka consumers and producers"""
    global consumer_manager, producer_manager
    
    try:
        # Setup Kafka Consumer
        consumer_manager = KafkaConsumerManager()
        
        submission_consumer = KafkaSubmissionConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_submission_topic,
            group_id=settings.kafka_consumer_group,
            message_handler=handle_submission_message
        )
        
        consumer_manager.add_consumer("submissions", submission_consumer)
        await consumer_manager.start_all()
        logger.info("Kafka consumers started successfully")
        
        # Setup Kafka Producer
        producer_manager = KafkaProducerManager()
        
        feedback_producer = KafkaFeedbackProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_feedback_topic
        )
        
        producer_manager.add_producer("feedback", feedback_producer)
        await producer_manager.start_all()
        logger.info("Kafka producers started successfully")
        
    except Exception as e:
        logger.error(f"Failed to setup Kafka: {str(e)}")
        raise


async def handle_submission_message(submission):
    """Handle incoming submission messages from Kafka"""
    try:
        logger.info(f"Processing submission: {submission.id}")
        
        # Evaluate the submission using the use case
        feedback = await evaluate_submission_use_case.execute(submission)
        
        # Publish feedback to Kafka
        feedback_producer = producer_manager.producers["feedback"]
        await feedback_producer.publish_feedback(feedback)
        
        logger.info(f"Successfully processed submission: {submission.id}")
        
    except Exception as e:
        logger.error(f"Error processing submission {submission.id}: {str(e)}")


async def cleanup_dependencies():
    """Cleanup all dependencies"""
    global mongo_client, llm_gateway, consumer_manager, producer_manager
    
    try:
        # Stop Kafka components
        if consumer_manager:
            await consumer_manager.stop_all()
            logger.info("Kafka consumers stopped")
        
        if producer_manager:
            await producer_manager.stop_all()
            logger.info("Kafka producers stopped")
        
        # Close LLM Gateway
        if llm_gateway:
            await llm_gateway.close()
            logger.info("LLM Gateway closed")
        
        # Close MongoDB connection
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed")
            
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting Feedback Service...")
    await setup_dependencies()
    logger.info("Feedback Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Feedback Service...")
    await cleanup_dependencies()
    logger.info("Feedback Service shutdown completed")


# Create FastAPI app
app = FastAPI(
    title="Feedback Service",
    description="Service for generating and managing test feedback using LLM",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency overrides
def get_store_feedback_use_case_override() -> StoreFeedbackUseCase:
    return store_feedback_use_case

def get_evaluate_submission_use_case_override() -> EvaluateSubmissionUseCase:
    return evaluate_submission_use_case

# Override the dependencies
app.dependency_overrides[get_store_feedback_use_case] = get_store_feedback_use_case_override
app.dependency_overrides[get_evaluate_submission_use_case] = get_evaluate_submission_use_case_override

# Include routers
app.include_router(feedback_router)

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "Feedback Service API",
        "version": "1.0.0",
        "status": "running"
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    """Comprehensive health check"""
    health_status = {
        "status": "healthy",
        "timestamp": "2024-01-01T00:00:00Z",
        "services": {}
    }
    
    try:
        # Check MongoDB
        if mongo_client:
            await mongo_client.admin.command('ping')
            health_status["services"]["mongodb"] = "healthy"
        else:
            health_status["services"]["mongodb"] = "not_initialized"
        
        # Check LLM Gateway
        if llm_gateway:
            llm_healthy = await llm_gateway.is_healthy()
            health_status["services"]["llm"] = "healthy" if llm_healthy else "unhealthy"
        else:
            health_status["services"]["llm"] = "not_initialized"
        
        # Check Kafka
        if consumer_manager and producer_manager:
            consumer_status = await consumer_manager.get_status()
            producer_status = await producer_manager.get_status()
            
            kafka_healthy = all(
                status.get("status") == "running" 
                for status in {**consumer_status, **producer_status}.values()
            )
            health_status["services"]["kafka"] = "healthy" if kafka_healthy else "unhealthy"
        else:
            health_status["services"]["kafka"] = "not_initialized"
        
        # Overall status
        all_healthy = all(
            status in ["healthy", "not_initialized"] 
            for status in health_status["services"].values()
        )
        health_status["status"] = "healthy" if all_healthy else "degraded"
        
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        health_status["status"] = "unhealthy"
        health_status["error"] = str(e)
    
    return health_status

# Exception handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}")
    return {
        "error": "Internal server error",
        "message": "An unexpected error occurred"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level="info"
    )