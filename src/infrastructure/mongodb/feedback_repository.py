import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError

from ...core.interfaces.feedback_repository import FeedbackRepository
from ...core.entities.feedback import Feedback, FeedbackStatus

logger = logging.getLogger(__name__)


class MongoFeedbackRepository(FeedbackRepository):
    """MongoDB implementation of FeedbackRepository"""
    
    def __init__(self, client: AsyncIOMotorClient, database_name: str):
        self.client = client
        self.db = client[database_name]
        self.collection: AsyncIOMotorCollection = self.db.feedback
        
    async def initialize(self):
        """Initialize indexes for optimal performance"""
        try:
            indexes = [
                IndexModel([("submission_id", ASCENDING)], unique=True),
                IndexModel([("user_id", ASCENDING)]),
                IndexModel([("status", ASCENDING)]),
                IndexModel([("created_at", DESCENDING)]),
                IndexModel([("updated_at", DESCENDING)])
            ]
            await self.collection.create_indexes(indexes)
            logger.info("MongoDB indexes created successfully")
        except Exception as e:
            logger.error(f"Failed to create indexes: {str(e)}")
    
    def _document_to_feedback(self, doc: Dict[str, Any]) -> Feedback:
        """Convert MongoDB document to Feedback entity"""
        if not doc:
            return None
        
        # Parse detailed feedback if present
        detailed_feedback = None
        if doc.get("feedback"):
            detailed_feedback = self._parse_detailed_feedback_from_doc(doc["feedback"])
        
        return Feedback(
            id=doc.get("_id"),
            submission_id=doc.get("submission_id"),
            user_id=doc.get("user_id"),
            generated_at=doc.get("generated_at", datetime.utcnow()),
            llm_model=doc.get("llm_model", ""),
            feedback=detailed_feedback,
            raw_llm_output=doc.get("raw_llm_output", ""),
            processing_time=doc.get("processing_time", 0.0),
            status=FeedbackStatus(doc.get("status", FeedbackStatus.PENDING.value)),
            metadata=doc.get("metadata", {}),
            created_at=doc.get("created_at", datetime.utcnow()),
            updated_at=doc.get("updated_at", datetime.utcnow())
        )
    
    def _feedback_to_document(self, feedback: Feedback) -> Dict[str, Any]:
        """Convert Feedback entity to MongoDB document"""
        doc = {
            "_id": feedback.id,
            "submission_id": feedback.submission_id,
            "user_id": feedback.user_id,
            "generated_at": feedback.generated_at,
            "llm_model": feedback.llm_model,
            "raw_llm_output": feedback.raw_llm_output,
            "processing_time": feedback.processing_time,
            "status": feedback.status.value,
            "metadata": feedback.metadata,
            "created_at": feedback.created_at,
            "updated_at": feedback.updated_at
        }
        
        # Convert detailed feedback to document format
        if feedback.feedback:
            doc["feedback"] = self._detailed_feedback_to_doc(feedback.feedback)
        
        return doc
    
    def _parse_detailed_feedback_from_doc(self, feedback_doc: Dict[str, Any]) -> 'DetailedFeedback':
        """Parse detailed feedback from MongoDB document"""
        from ...core.entities.feedback import DetailedFeedback, CriteriaFeedback, SentenceFeedback
        
        # Parse criteria feedback
        def parse_criteria(criteria_doc):
            return CriteriaFeedback(
                score=criteria_doc.get("score", 0),
                comments=criteria_doc.get("comments", ""),
                suggestions=criteria_doc.get("suggestions", [])
            )
        
        # Parse sentence feedback
        sentence_feedback = []
        for sent_doc in feedback_doc.get("sentence_feedback", []):
            feedback_details = {}
            for criteria, detail_doc in sent_doc.get("feedback_details", {}).items():
                feedback_details[criteria] = CriteriaFeedback(
                    score=0,
                    comments=detail_doc.get("comments", ""),
                    suggestions=detail_doc.get("suggestions", [])
                )
            
            sentence_feedback.append(SentenceFeedback(
                sentence_id=sent_doc.get("sentence_id", ""),
                text=sent_doc.get("text", ""),
                improved_text=sent_doc.get("improved_text", ""),
                feedback_details=feedback_details
            ))
        
        return DetailedFeedback(
            overall_score=feedback_doc.get("overall_score", 0),
            task_achievement=parse_criteria(feedback_doc.get("task_achievement", {})),
            coherence_cohesion=parse_criteria(feedback_doc.get("coherence_cohesion", {})),
            lexical_resource=parse_criteria(feedback_doc.get("lexical_resource", {})),
            grammatical_range_accuracy=parse_criteria(feedback_doc.get("grammatical_range_accuracy", {})),
            sentence_feedback=sentence_feedback
        )
    
    def _detailed_feedback_to_doc(self, detailed_feedback: 'DetailedFeedback') -> Dict[str, Any]:
        """Convert DetailedFeedback to MongoDB document format"""
        def criteria_to_doc(criteria):
            return {
                "score": criteria.score,
                "comments": criteria.comments,
                "suggestions": criteria.suggestions
            }
        
        sentence_feedback_docs = []
        for sent_feedback in detailed_feedback.sentence_feedback:
            feedback_details_doc = {}
            for criteria, detail in sent_feedback.feedback_details.items():
                feedback_details_doc[criteria] = {
                    "comments": detail.comments,
                    "suggestions": detail.suggestions
                }
            
            sentence_feedback_docs.append({
                "sentence_id": sent_feedback.sentence_id,
                "text": sent_feedback.text,
                "improved_text": sent_feedback.improved_text,
                "feedback_details": feedback_details_doc
            })
        
        return {
            "overall_score": detailed_feedback.overall_score,
            "task_achievement": criteria_to_doc(detailed_feedback.task_achievement),
            "coherence_cohesion": criteria_to_doc(detailed_feedback.coherence_cohesion),
            "lexical_resource": criteria_to_doc(detailed_feedback.lexical_resource),
            "grammatical_range_accuracy": criteria_to_doc(detailed_feedback.grammatical_range_accuracy),
            "sentence_feedback": sentence_feedback_docs
        }
    
    async def save(self, feedback: Feedback) -> Feedback:
        """Save a feedback entity"""
        try:
            doc = self._feedback_to_document(feedback)
            
            # Use upsert to handle both insert and update
            result = await self.collection.replace_one(
                {"_id": feedback.id},
                doc,
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Inserted new feedback: {feedback.id}")
            else:
                logger.info(f"Updated feedback: {feedback.id}")
            
            return feedback
            
        except PyMongoError as e:
            logger.error(f"Failed to save feedback {feedback.id}: {str(e)}")
            raise RepositoryError(f"Failed to save feedback: {str(e)}")
    
    async def find_by_id(self, feedback_id: str) -> Optional[Feedback]:
        """Find feedback by ID"""
        try:
            doc = await self.collection.find_one({"_id": feedback_id})
            return self._document_to_feedback(doc) if doc else None
            
        except PyMongoError as e:
            logger.error(f"Failed to find feedback {feedback_id}: {str(e)}")
            raise RepositoryError(f"Failed to find feedback: {str(e)}")
    
    async def find_by_submission_id(self, submission_id: str) -> Optional[Feedback]:
        """Find feedback by submission ID"""
        try:
            doc = await self.collection.find_one({"submission_id": submission_id})
            return self._document_to_feedback(doc) if doc else None
            
        except PyMongoError as e:
            logger.error(f"Failed to find feedback for submission {submission_id}: {str(e)}")
            raise RepositoryError(f"Failed to find feedback: {str(e)}")
    
    async def find_by_user_id(self, user_id: str, limit: int = 10, offset: int = 0) -> List[Feedback]:
        """Find feedback by user ID with pagination"""
        try:
            cursor = self.collection.find({"user_id": user_id}).sort("created_at", DESCENDING).skip(offset).limit(limit)
            docs = await cursor.to_list(length=limit)
            return [self._document_to_feedback(doc) for doc in docs]
            
        except PyMongoError as e:
            logger.error(f"Failed to find feedback for user {user_id}: {str(e)}")
            raise RepositoryError(f"Failed to find feedback: {str(e)}")
    
    async def find_by_status(self, status: FeedbackStatus, limit: int = 10, offset: int = 0) -> List[Feedback]:
        """Find feedback by status with pagination"""
        try:
            cursor = self.collection.find({"status": status.value}).sort("created_at", DESCENDING).skip(offset).limit(limit)
            docs = await cursor.to_list(length=limit)
            return [self._document_to_feedback(doc) for doc in docs]
            
        except PyMongoError as e:
            logger.error(f"Failed to find feedback with status {status.value}: {str(e)}")
            raise RepositoryError(f"Failed to find feedback: {str(e)}")
    
    async def update_status(self, feedback_id: str, status: FeedbackStatus) -> bool:
        """Update feedback status"""
        try:
            result = await self.collection.update_one(
                {"_id": feedback_id},
                {
                    "$set": {
                        "status": status.value,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            return result.modified_count > 0
            
        except PyMongoError as e:
            logger.error(f"Failed to update feedback {feedback_id} status: {str(e)}")
            raise RepositoryError(f"Failed to update feedback status: {str(e)}")
    
    async def delete(self, feedback_id: str) -> bool:
        """Delete feedback by ID"""
        try:
            result = await self.collection.delete_one({"_id": feedback_id})
            return result.deleted_count > 0
            
        except PyMongoError as e:
            logger.error(f"Failed to delete feedback {feedback_id}: {str(e)}")
            raise RepositoryError(f"Failed to delete feedback: {str(e)}")
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get repository statistics"""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$status",
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            cursor = self.collection.aggregate(pipeline)
            stats = {"total": 0, "by_status": {}}
            
            async for doc in cursor:
                stats["by_status"][doc["_id"]] = doc["count"]
                stats["total"] += doc["count"]
            
            return stats
            
        except PyMongoError as e:
            logger.error(f"Failed to get repository stats: {str(e)}")
            raise RepositoryError(f"Failed to get stats: {str(e)}")


class RepositoryError(Exception):
    """Exception raised when repository operations fail"""
    pass