import logging
from typing import Dict, Any, Optional
from ..entities.feedback import Submission, Feedback, FeedbackStatus
from ..interfaces.in_feedback_repository import FeedbackRepository
from ..interfaces.in_llm_gateway import LLMGateway, LLMGatewayError

logger = logging.getLogger(__name__)


class EvaluateSubmissionUseCase:
    """Use case for evaluating submissions and generating feedback"""
    
    def __init__(self, feedback_repository: FeedbackRepository, llm_gateway: LLMGateway):
        self.feedback_repository = feedback_repository
        self.llm_gateway = llm_gateway
    
    async def execute(self, submission: Submission, criteria: Optional[Dict[str, Any]] = None) -> Feedback:
        """
        Execute the evaluation process for a submission
        
        Args:
            submission: The submission to evaluate
            criteria: Optional evaluation criteria
            
        Returns:
            Feedback: The generated feedback
            
        Raises:
            EvaluationError: If evaluation fails
        """
        logger.info(f"Starting evaluation for submission {submission.id}")
        
        # Check if feedback already exists
        existing_feedback = await self.feedback_repository.find_by_submission_id(submission.id)
        if existing_feedback and existing_feedback.is_completed():
            logger.info(f"Feedback already exists for submission {submission.id}")
            return existing_feedback
        
        # Create initial feedback record
        feedback = Feedback(
            submission_id=submission.id,
            user_id=submission.user_id,
            status=FeedbackStatus.PROCESSING,
            metadata={"submission_type": submission.submission_type.value}
        )
        
        try:
            # Save initial feedback
            feedback = await self.feedback_repository.save(feedback)
            logger.info(f"Created feedback record {feedback.id}")
            
            # Generate feedback using LLM
            generated_feedback = await self.llm_gateway.generate_feedback(submission)
            
            # Update feedback with generated content
            feedback.content = generated_feedback.content
            feedback.score = generated_feedback.score
            feedback.suggestions = generated_feedback.suggestions
            feedback.strengths = generated_feedback.strengths
            feedback.weaknesses = generated_feedback.weaknesses
            feedback.update_status(FeedbackStatus.COMPLETED)
            
            # Save completed feedback
            feedback = await self.feedback_repository.save(feedback)
            logger.info(f"Successfully generated feedback for submission {submission.id}")
            
            return feedback
            
        except LLMGatewayError as e:
            logger.error(f"LLM gateway error for submission {submission.id}: {str(e)}")
            feedback.update_status(FeedbackStatus.FAILED)
            feedback.metadata["error"] = str(e)
            await self.feedback_repository.save(feedback)
            raise EvaluationError(f"Failed to generate feedback: {str(e)}")
        
        except Exception as e:
            logger.error(f"Unexpected error for submission {submission.id}: {str(e)}")
            feedback.update_status(FeedbackStatus.FAILED)
            feedback.metadata["error"] = str(e)
            await self.feedback_repository.save(feedback)
            raise EvaluationError(f"Unexpected error during evaluation: {str(e)}")


class EvaluationError(Exception):
    """Exception raised when evaluation fails"""
    pass