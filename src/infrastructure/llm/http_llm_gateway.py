import logging
import json
from typing import Dict, Any, Optional
import aiohttp
import asyncio
from datetime import datetime
from openai import AsyncOpenAI
from ...core.interfaces.in_llm_gateway import LLMGateway, LLMGatewayError, LLMServiceUnavailableError
from ...core.entities.feedback import ExamType, SkillType, Submission, Feedback, FeedbackStatus, SubmissionType,DetailedFeedback

logger = logging.getLogger(__name__)


class HttpLLMGateway(LLMGateway):
    """HTTP implementation of LLMGateway for external LLM APIs"""
    
    def __init__(self, base_url: str, api_key: str, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.session = None
        
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                }
            )
        return self.session
    
    async def close(self):
        """Close HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    def _build_prompt(self, submission: Submission) -> str:
        """Build prompt for LLM based on submission type"""

        if submission.skill_type == SkillType.WRITING:
    
            return f"""
topic: {submission.topic}
{submission.content}
            """
        
        else:  # Default for other submission types
            return f"""
Analyze this {submission.submission_type.value} submission and provide detailed feedback:

Topic: {submission.topic}
Content:
{submission.content}
            """
    
    async def generate_feedback(self, submission: Submission) -> Feedback:
        """Generate feedback for a submission using LLM"""
        try:
            completion = await self.client.responses.parse(
                model="gpt-5",  
                input=[
                    {
                        "role": "system",
                        "content": self._system_prompt_essay_IELTS_writing_task_2()
                        if submission.submission_type == SubmissionType.ESSAY else ""
                    },
                    {
                        "role": "user",
                        "content": self._build_prompt(submission)
                    }
                ],
                response_format=DetailedFeedback,
            )

            if completion.refusal:
                raise LLMGatewayError(f"Model refused: {completion.refusal}")
            
            detailed_feedback = completion.output_parsed

            return Feedback(
                submission_id=submission.id,
                user_id=submission.user_id,
                skill_type=submission.skill_type,
                exam_type=submission.exam_type,
                assignment_id=submission.assignment_id,
                topic=submission.topic,
                task_number=submission.task_number,
                generated_at=datetime.utcnow(),
                llm_model=completion.model or "unknown",
                feedback=detailed_feedback,
                processing_time=(completion.usage.total_tokens / 1000.0) if completion.usage else 0,
                status=FeedbackStatus.COMPLETED,
                metadata={
                    "tokens_used": completion.usage.total_tokens if completion.usage else 0,
                    "submission_type": submission.submission_type.value,
                    "confidence_score": 0.85
                }
            )

        except LLMServiceUnavailableError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in generate_feedback: {str(e)}")
            raise LLMGatewayError(f"Unexpected error: {str(e)}")
    
    def _parse_detailed_feedback(self, feedback_json: Dict[str, Any]) -> DetailedFeedback:
        """Parse JSON feedback into DetailedFeedback entity"""
        from ...core.entities.feedback import DetailedFeedback, CriteriaFeedback, SentenceFeedback
        
        # Parse criteria feedback
        task_achievement = CriteriaFeedback(
            score=feedback_json.get("task_achievement", {}).get("score", 0),
            comments=feedback_json.get("task_achievement", {}).get("comments", ""),
            suggestions=feedback_json.get("task_achievement", {}).get("suggestions", [])
        )
        
        coherence_cohesion = CriteriaFeedback(
            score=feedback_json.get("coherence_cohesion", {}).get("score", 0),
            comments=feedback_json.get("coherence_cohesion", {}).get("comments", ""),
            suggestions=feedback_json.get("coherence_cohesion", {}).get("suggestions", [])
        )
        
        lexical_resource = CriteriaFeedback(
            score=feedback_json.get("lexical_resource", {}).get("score", 0),
            comments=feedback_json.get("lexical_resource", {}).get("comments", ""),
            suggestions=feedback_json.get("lexical_resource", {}).get("suggestions", [])
        )
        
        grammatical_range_accuracy = CriteriaFeedback(
            score=feedback_json.get("grammatical_range_accuracy", {}).get("score", 0),
            comments=feedback_json.get("grammatical_range_accuracy", {}).get("comments", ""),
            suggestions=feedback_json.get("grammatical_range_accuracy", {}).get("suggestions", [])
        )
        
        # Parse sentence feedback
        sentence_feedback = []
        for sent_data in feedback_json.get("sentence_feedback", []):
            feedback_details = {}
            for criteria in ["task_achievement", "coherence_cohesion", "lexical_resource", "grammatical_range_accuracy"]:
                if criteria in sent_data.get("feedback_details", {}):
                    detail = sent_data["feedback_details"][criteria]
                    feedback_details[criteria] = CriteriaFeedback(
                        score=0,  # Sentence level doesn't have scores
                        comments=detail.get("comments", ""),
                        suggestions=detail.get("suggestions", [])
                    )
            
            sentence_feedback.append(SentenceFeedback(
                sentence_id=sent_data.get("sentence_id", ""),
                text=sent_data.get("text", ""),
                improved_text=sent_data.get("improved_text", ""),
                feedback_details=feedback_details
            ))
        
        return DetailedFeedback(
            overall_score=feedback_json.get("overall_score", 0),
            task_achievement=task_achievement,
            coherence_cohesion=coherence_cohesion,
            lexical_resource=lexical_resource,
            grammatical_range_accuracy=grammatical_range_accuracy,
            sentence_feedback=sentence_feedback
        )
    
    def _create_fallback_feedback(self, content: str) -> DetailedFeedback:
        """Create basic feedback when JSON parsing fails"""
        from ...core.entities.feedback import DetailedFeedback, CriteriaFeedback
        
        # Create basic criteria feedback
        basic_criteria = CriteriaFeedback(
            score=5.0,  # Default middle score
            comments=content[:500] + "..." if len(content) > 500 else content,
            suggestions=["Review the detailed feedback above"]
        )
        
        return DetailedFeedback(
            overall_score=5.0,
            task_achievement=basic_criteria,
            coherence_cohesion=basic_criteria,
            lexical_resource=basic_criteria,
            grammatical_range_accuracy=basic_criteria,
            sentence_feedback=[]
        )
            
        # except KeyError as e:
        #     logger.error(f"Failed to parse LLM response: missing key {str(e)}")
        #     raise LLMGatewayError(f"Invalid response format: missing {str(e)}")
        # except Exception as e:
        #     logger.error(f"Failed to parse LLM response: {str(e)}")
        #     raise LLMGatewayError(f"Response parsing error: {str(e)}")
    
    async def evaluate_submission(self, submission: Submission, criteria: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Evaluate submission and return detailed analysis"""
        try:
            session = await self._get_session()
            
            # Build evaluation prompt with criteria
            evaluation_prompt = self._build_evaluation_prompt(submission, criteria)
            
            payload = {
                "model": "gpt-3.5-turbo",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert evaluator. Provide detailed analysis in JSON format."
                    },
                    {
                        "role": "user",
                        "content": evaluation_prompt
                    }
                ],
                "temperature": 0.3,
                "max_tokens": 800
            }
            
            async with session.post(f"{self.base_url}/response", json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    content = data["choices"][0]["message"]["content"]
                    
                    # Try to parse as JSON, fallback to structured text
                    try:
                        return json.loads(content)
                    except json.JSONDecodeError:
                        return {"raw_analysis": content, "structured": False}
                else:
                    error_text = await response.text()
                    raise LLMGatewayError(f"Evaluation failed: {response.status} - {error_text}")
                    
        except Exception as e:
            logger.error(f"Evaluation error: {str(e)}")
            raise LLMGatewayError(f"Evaluation failed: {str(e)}")
    
    def _build_evaluation_prompt(self, submission: Submission, criteria: Optional[Dict[str, Any]]) -> str:
        """Build evaluation prompt with optional criteria"""
        base_prompt = f"""
Please evaluate this {submission.submission_type.value} submission:

Content:
{submission.content}

Provide analysis in JSON format with the following structure:
{{
    "overall_score": <score_out_of_100>,
    "criteria_scores": {{
        "quality": <score>,
        "accuracy": <score>,
        "completeness": <score>
    }},
    "strengths": [<list_of_strengths>],
    "weaknesses": [<list_of_weaknesses>],
    "suggestions": [<list_of_suggestions>],
    "summary": "<brief_summary>"
}}
        """
        
        if criteria:
            criteria_text = "\n".join([f"- {k}: {v}" for k, v in criteria.items()])
            base_prompt += f"\n\nAdditional evaluation criteria:\n{criteria_text}"
        
        return base_prompt
    

    def _system_prompt_essay_IELTS_writing_task_2(self) -> str:
        return """
You are an IELTS examiner. Evaluate essays strictly by IELTS Writing Task 2 band descriptors.
Providing general feedback: overall score,task achievement : how well task is fulfilled,coherence cohesion: how ideas are organized and linked,lexical resource: vocabulary range and accuracy,grammatical range accuracy: grammar structures and accuracy for essay. 
Feedback for each sentence follow that all criteria, help student improve their writing by provide improved version  and explain why it improves.

"""


