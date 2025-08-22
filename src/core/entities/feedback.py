from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
import uuid


class FeedbackStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class SubmissionType(Enum):
    CODE = "code"
    ESSAY = "essay"
    QUIZ = "quiz"
    ASSIGNMENT = "assignment"


@dataclass
class Submission:
    """Entity representing a test submission to be evaluated"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str = ""
    content: str = ""
    submission_type: SubmissionType = SubmissionType.CODE
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        if isinstance(self.submission_type, str):
            self.submission_type = SubmissionType(self.submission_type)


@dataclass
class CriteriaFeedback:
    """Feedback for a specific criteria"""
    score: float
    comments: str
    suggestions: list[str] = field(default_factory=list)


@dataclass
class SentenceFeedback:
    """Detailed feedback for individual sentences"""
    sentence_id: str
    text: str
    improved_text: str
    feedback_details: Dict[str, CriteriaFeedback] = field(default_factory=dict)


@dataclass
class DetailedFeedback:
    """Structured detailed feedback"""
    overall_score: float
    task_achievement: CriteriaFeedback
    coherence_cohesion: CriteriaFeedback
    lexical_resource: CriteriaFeedback
    grammatical_range_accuracy: CriteriaFeedback
    sentence_feedback: list[SentenceFeedback] = field(default_factory=list)


@dataclass
class Feedback:
    """Entity representing generated feedback for a submission"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    submission_id: str = ""
    user_id: str = ""
    generated_at: datetime = field(default_factory=datetime.utcnow)
    llm_model: str = ""
    feedback: Optional[DetailedFeedback] = None
    raw_llm_output: str = ""
    processing_time: float = 0.0
    status: FeedbackStatus = FeedbackStatus.PENDING
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        if isinstance(self.status, str):
            self.status = FeedbackStatus(self.status)
    
    def update_status(self, status: FeedbackStatus):
        """Update feedback status and timestamp"""
        self.status = status
        self.updated_at = datetime.utcnow()
    
    def is_completed(self) -> bool:
        """Check if feedback generation is completed"""
        return self.status == FeedbackStatus.COMPLETED
    
    def is_failed(self) -> bool:
        """Check if feedback generation failed"""
        return self.status == FeedbackStatus.FAILED