from abc import ABC, abstractmethod
from typing import Optional, List
from ..entities.feedback import Feedback, FeedbackStatus


class FeedbackRepository(ABC):
    """Abstract repository interface for feedback operations"""
    
    @abstractmethod
    async def save(self, feedback: Feedback) -> Feedback:
        pass
    
    @abstractmethod
    async def find_by_id(self, feedback_id: str) -> Optional[Feedback]:
        pass
    
    @abstractmethod
    async def find_by_submission_id(self, submission_id: str) -> Optional[Feedback]:
        pass
    
    @abstractmethod
    async def find_by_user_id(self, user_id: str, limit: int = 10, offset: int = 0) -> List[Feedback]:
        pass
    
    @abstractmethod
    async def find_by_status(self, status: FeedbackStatus, limit: int = 10, offset: int = 0) -> List[Feedback]:
        pass
    
    @abstractmethod
    async def update_status(self, feedback_id: str, status: FeedbackStatus) -> bool:
        pass
    
    @abstractmethod
    async def delete(self, feedback_id: str) -> bool:
        pass