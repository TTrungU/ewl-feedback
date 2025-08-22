from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

from ...core.entities.feedback import Feedback, FeedbackStatus, Submission
from ...core.use_cases.store_feedback import StoreFeedbackUseCase, StoreFeedbackError
from ...core.use_cases.evaluate_submission import EvaluateSubmissionUseCase, EvaluationError

# Pydantic models for API
class CriteriaFeedbackResponse(BaseModel):
    score: float
    comments: str
    suggestions: List[str] = []

class SentenceFeedbackResponse(BaseModel):
    sentence_id: str
    text: str
    improved_text: str
    feedback_details: Dict[str, Dict[str, Any]] = {}

class DetailedFeedbackResponse(BaseModel):
    overall_score: float
    task_achievement: CriteriaFeedbackResponse
    coherence_cohesion: CriteriaFeedbackResponse
    lexical_resource: CriteriaFeedbackResponse
    grammatical_range_accuracy: CriteriaFeedbackResponse
    sentence_feedback: List[SentenceFeedbackResponse] = []

class FeedbackResponse(BaseModel):
    feedback_id: str = Field(alias="id")
    submission_id: str
    user_id: str
    generated_at: datetime
    llm_model: str
    feedback: Optional[DetailedFeedbackResponse] = None
    raw_llm_output: str = ""
    processing_time: float
    status: str
    metadata: Dict[str, Any] = {}

    class Config:
        allow_population_by_field_name = True

class FeedbackListResponse(BaseModel):
    feedbacks: List[FeedbackResponse]
    total: int
    page: int
    limit: int

class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    services: Dict[str, Any]

class ErrorResponse(BaseModel):
    error: str
    message: str
    timestamp: datetime

# Dependency injection (to be configured in main.py)
def get_store_feedback_use_case() -> StoreFeedbackUseCase:
    # This will be overridden in main.py with actual dependencies
    raise NotImplementedError("Dependency not configured")

def get_evaluate_submission_use_case() -> EvaluateSubmissionUseCase:
    # This will be overridden in main.py with actual dependencies
    raise NotImplementedError("Dependency not configured")

# Router
router = APIRouter(prefix="/feedback", tags=["feedback"])

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow(),
        services={
            "api": "running",
            "database": "connected",
            "kafka": "connected"
        }
    )

@router.post("/", response_model=FeedbackResponse)
async def create_feedback(
    submission: Submission,
    store_use_case: StoreFeedbackUseCase = Depends(get_store_feedback_use_case)
):
    """Create new feedback"""
    try:
        feedback = await store_use_case.create(submission)
        return _feedback_to_response(feedback)
    except StoreFeedbackError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/{feedback_id}", response_model=FeedbackResponse)
async def get_feedback(
    feedback_id: str,
    store_use_case: StoreFeedbackUseCase = Depends(get_store_feedback_use_case)
):
    """Get feedback by ID"""
    try:
        feedback = await store_use_case.get_by_id(feedback_id)
        
        if not feedback:
            raise HTTPException(
                status_code=404,
                detail=f"Feedback with ID {feedback_id} not found"
            )
        
        return _feedback_to_response(feedback)
        
    except StoreFeedbackError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/submission/{submission_id}", response_model=FeedbackResponse)
async def get_feedback_by_submission(
    submission_id: str,
    store_use_case: StoreFeedbackUseCase = Depends(get_store_feedback_use_case)
):
    """Get feedback by submission ID"""
    try:
        feedback = await store_use_case.get_by_submission_id(submission_id)
        
        if not feedback:
            raise HTTPException(
                status_code=404,
                detail=f"Feedback for submission {submission_id} not found"
            )
        
        return _feedback_to_response(feedback)
        
    except StoreFeedbackError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/user/{user_id}", response_model=FeedbackListResponse)
async def get_user_feedback(
    user_id: str,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    store_use_case: StoreFeedbackUseCase = Depends(get_store_feedback_use_case)
):
    """Get feedback for a specific user with pagination"""
    try:
        offset = (page - 1) * limit
        feedbacks = await store_use_case.get_by_user_id(user_id, limit, offset)
        
        feedback_responses = [_feedback_to_response(f) for f in feedbacks]
        
        return FeedbackListResponse(
            feedbacks=feedback_responses,
            total=len(feedbacks),  # In a real app, you'd get the total count separately
            page=page,
            limit=limit
        )
        
    except StoreFeedbackError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.put("/{feedback_id}/status", response_model=Dict[str, Any])
async def update_feedback_status(
    feedback_id: str,
    status: FeedbackStatus,
    store_use_case: StoreFeedbackUseCase = Depends(get_store_feedback_use_case)
):
    """Update feedback status"""
    try:
        success = await store_use_case.update_status(feedback_id, status)
        
        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Feedback with ID {feedback_id} not found or could not be updated"
            )
        
        return {"message": "Status updated successfully", "feedback_id": feedback_id, "new_status": status.value}
        
    except StoreFeedbackError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/", response_model=FeedbackListResponse)
async def list_feedback(
    status: Optional[FeedbackStatus] = Query(None, description="Filter by status"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    store_use_case: StoreFeedbackUseCase = Depends(get_store_feedback_use_case)
):
    """List feedback with optional status filter and pagination"""
    try:
        offset = (page - 1) * limit
        
        if status:
            # Get feedback by status (this would need to be implemented in the use case)
            feedbacks = []  # Placeholder - implement status filtering
        else:
            # For now, we'll return empty list as we don't have a list all method
            feedbacks = []
        
        feedback_responses = [_feedback_to_response(f) for f in feedbacks]
        
        return FeedbackListResponse(
            feedbacks=feedback_responses,
            total=len(feedbacks),
            page=page,
            limit=limit
        )
        
    except StoreFeedbackError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.delete("/{feedback_id}", response_model=Dict[str, Any])
async def delete_feedback(
    feedback_id: str,
    store_use_case: StoreFeedbackUseCase = Depends(get_store_feedback_use_case)
):
    """Delete feedback by ID"""
    try:
        # First check if feedback exists
        feedback = await store_use_case.get_by_id(feedback_id)
        if not feedback:
            raise HTTPException(
                status_code=404,
                detail=f"Feedback with ID {feedback_id} not found"
            )
        
        # Note: Delete functionality would need to be added to the use case
        # For now, return a placeholder response
        return {
            "message": "Delete functionality not implemented yet",
            "feedback_id": feedback_id
        }
        
    except StoreFeedbackError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/stats/overview", response_model=Dict[str, Any])
async def get_feedback_stats():
    """Get feedback statistics overview"""
    try:
        # This would be implemented with proper repository methods
        stats = {
            "total_feedback": 0,
            "by_status": {
                "pending": 0,
                "processing": 0,
                "completed": 0,
                "failed": 0
            },
            "average_score": 0.0,
            "last_updated": datetime.utcnow()
        }
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

def _feedback_to_response(feedback: Feedback) -> FeedbackResponse:
    """Convert Feedback entity to response model"""
    detailed_feedback_response = None
    
    if feedback.feedback:
        # Convert detailed feedback
        def criteria_to_response(criteria):
            return CriteriaFeedbackResponse(
                score=criteria.score,
                comments=criteria.comments,
                suggestions=criteria.suggestions
            )
        
        sentence_feedback_responses = []
        for sent_feedback in feedback.feedback.sentence_feedback:
            feedback_details = {}
            for criteria, detail in sent_feedback.feedback_details.items():
                feedback_details[criteria] = {
                    "comments": detail.comments,
                    "suggestions": detail.suggestions
                }
            
            sentence_feedback_responses.append(SentenceFeedbackResponse(
                sentence_id=sent_feedback.sentence_id,
                text=sent_feedback.text,
                improved_text=sent_feedback.improved_text,
                feedback_details=feedback_details
            ))
        
        detailed_feedback_response = DetailedFeedbackResponse(
            overall_score=feedback.feedback.overall_score,
            task_achievement=criteria_to_response(feedback.feedback.task_achievement),
            coherence_cohesion=criteria_to_response(feedback.feedback.coherence_cohesion),
            lexical_resource=criteria_to_response(feedback.feedback.lexical_resource),
            grammatical_range_accuracy=criteria_to_response(feedback.feedback.grammatical_range_accuracy),
            sentence_feedback=sentence_feedback_responses
        )
    
    return FeedbackResponse(
        feedback_id=feedback.id,
        submission_id=feedback.submission_id,
        user_id=feedback.user_id,
        generated_at=feedback.generated_at,
        llm_model=feedback.llm_model,
        feedback=detailed_feedback_response,
        raw_llm_output=feedback.raw_llm_output,
        processing_time=feedback.processing_time,
        status=feedback.status.value,
        metadata=feedback.metadata
    )