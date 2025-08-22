import logging
from typing import Optional, List
from ..entities.feedback import Feedback, FeedbackStatus
from ..interfaces.in_feedback_repository import FeedbackRepository

logger = logging.getLogger(__name__)


class StoreFeedbackUseCase:
    """Use case for storing and retrieving feedback"""
    
    def __init__(self, feedback_repository: FeedbackRepository):
        self.feedback_repository = feedback_repository
    
    async def store(self, feedback: Feedback) -> Feedback:
        """
        Store feedback in the repository
        
        Args:
            feedback: The feedback to store
            
        Returns:
            Feedback: The stored feedback
        """
        logger.info(f"Storing feedback {feedback.id}")
        
        try:
            stored_feedback = await self.feedback_repository.save(feedback)
            logger.info(f"Successfully stored feedback {feedback.id}")
            return stored_feedback
        
        except Exception as e:
            logger.error(f"Failed to store feedback {feedback.id}: {str(e)}")
            raise StoreFeedbackError(f"Failed to store feedback: {str(e)}")
    
    async def get_by_id(self, feedback_id: str) -> Optional[Feedback]:
        """
        Get feedback by ID
        
        Args:
            feedback_id: The feedback ID
            
        Returns:
            Optional[Feedback]: The feedback if found
        """
        logger.info(f"Retrieving feedback {feedback_id}")
        
        try:
            feedback = await self.feedback_repository.find_by_id(feedback_id)
            if feedback:
                logger.info(f"Found feedback {feedback_id}")
            else:
                logger.info(f"Feedback {feedback_id} not found")
            return feedback
        
        except Exception as e:
            logger.error(f"Failed to retrieve feedback {feedback_id}: {str(e)}")
            raise StoreFeedbackError(f"Failed to retrieve feedback: {str(e)}")
    
    async def get_by_submission_id(self, submission_id: str) -> Optional[Feedback]:
        """
        Get feedback by submission ID
        
        Args:
            submission_id: The submission ID
            
        Returns:
            Optional[Feedback]: The feedback if found
        """
        logger.info(f"Retrieving feedback for submission {submission_id}")
        
        try:
            feedback = await self.feedback_repository.find_by_submission_id(submission_id)
            if feedback:
                logger.info(f"Found feedback for submission {submission_id}")
            else:
                logger.info(f"No feedback found for submission {submission_id}")
            return feedback
        
        except Exception as e:
            logger.error(f"Failed to retrieve feedback for submission {submission_id}: {str(e)}")
            raise StoreFeedbackError(f"Failed to retrieve feedback: {str(e)}")
    
    async def get_by_user_id(self, user_id: str, limit: int = 10, offset: int = 0) -> List[Feedback]:
        """
        Get feedback by user ID with pagination
        
        Args:
            user_id: The user ID
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            List[Feedback]: List of feedback
        """
        logger.info(f"Retrieving feedback for user {user_id}")
        
        try:
            feedback_list = await self.feedback_repository.find_by_user_id(user_id, limit, offset)
            logger.info(f"Found {len(feedback_list)} feedback records for user {user_id}")
            return feedback_list
        
        except Exception as e:
            logger.error(f"Failed to retrieve feedback for user {user_id}: {str(e)}")
            raise StoreFeedbackError(f"Failed to retrieve feedback: {str(e)}")
    
    async def update_status(self, feedback_id: str, status: FeedbackStatus) -> bool:
        """
        Update feedback status
        
        Args:
            feedback_id: The feedback ID
            status: New status
            
        Returns:
            bool: True if updated successfully
        """
        logger.info(f"Updating feedback {feedback_id} status to {status.value}")
        
        try:
            success = await self.feedback_repository.update_status(feedback_id, status)
            if success:
                logger.info(f"Successfully updated feedback {feedback_id} status")
            else:
                logger.warning(f"Failed to update feedback {feedback_id} status")
            return success
        
        except Exception as e:
            logger.error(f"Failed to update feedback {feedback_id} status: {str(e)}")
            raise StoreFeedbackError(f"Failed to update feedback status: {str(e)}")


class StoreFeedbackError(Exception):
    """Exception raised when storing feedback fails"""
    pass