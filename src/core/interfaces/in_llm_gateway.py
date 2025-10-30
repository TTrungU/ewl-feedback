from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from ..entities.feedback import Submission, Feedback


class LLMGateway(ABC):
    """Abstract gateway interface for LLM operations"""
    
    @abstractmethod
    async def generate_feedback(self, submission: Submission) -> Feedback:
        """Generate feedback for a submission using LLM"""
        pass
    
    @abstractmethod
    async def evaluate_submission(self, submission: Submission, criteria: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Evaluate submission and return detailed analysis"""
        pass
    
    @abstractmethod
    async def is_healthy(self) -> bool:
        """Check if LLM service is healthy"""
        pass


class LLMGatewayError(Exception):
    """Exception raised when LLM gateway operations fail"""
    pass


class LLMServiceUnavailableError(LLMGatewayError):
    """Exception raised when LLM service is unavailable"""
    pass