import logging
import json
from typing import Dict, Any, Optional
import aiohttp
import asyncio
from datetime import datetime

from ...core.interfaces.llm_gateway import LLMGateway, LLMGatewayError, LLMServiceUnavailableError
from ...core.entities.feedback import Submission, Feedback, FeedbackStatus, SubmissionType,DetailedFeedback

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
        
        if submission.submission_type == SubmissionType.ESSAY:
            return f"""
You are an expert IELTS Writing examiner. Analyze the following essay and provide detailed feedback in the specified JSON format.

Essay to evaluate:
{submission.content}

Please provide feedback following IELTS Writing Band Descriptors (0-9 scale) for these four criteria:
1. Task Achievement (TA): How well the task is fulfilled
2. Coherence and Cohesion (CC): Organization and linking of ideas
3. Lexical Resource (LR): Vocabulary range and accuracy
4. Grammatical Range and Accuracy (GRA): Grammar structures and accuracy

For each sentence, provide:
- Identification of errors and issues
- Improved version of the sentence
- Specific feedback for each criteria
- Actionable suggestions for improvement

Response format (JSON):
{{
  "overall_score": <average_score_out_of_9>,
  "task_achievement": {{
    "score": <score_0_to_9>,
    "comments": "<detailed_assessment>",
    "suggestions": ["<specific_suggestion_1>", "<specific_suggestion_2>"]
  }},
  "coherence_cohesion": {{
    "score": <score_0_to_9>,
    "comments": "<detailed_assessment>",
    "suggestions": ["<specific_suggestion_1>", "<specific_suggestion_2>"]
  }},
  "lexical_resource": {{
    "score": <score_0_to_9>,
    "comments": "<detailed_assessment>",
    "suggestions": ["<specific_suggestion_1>", "<specific_suggestion_2>"]
  }},
  "grammatical_range_accuracy": {{
    "score": <score_0_to_9>,
    "comments": "<detailed_assessment>",
    "suggestions": ["<specific_suggestion_1>", "<specific_suggestion_2>"]
  }},
  "sentence_feedback": [
    {{
      "sentence_id": "sent_001",
      "text": "<original_sentence>",
      "improved_text": "<corrected_sentence>",
      "feedback_details": {{
        "task_achievement": {{
          "comments": "<sentence_level_TA_feedback>",
          "suggestions": ["<specific_improvement>"]
        }},
        "coherence_cohesion": {{
          "comments": "<sentence_level_CC_feedback>",
          "suggestions": ["<specific_improvement>"]
        }},
        "lexical_resource": {{
          "comments": "<sentence_level_LR_feedback>",
          "suggestions": ["<specific_improvement>"]
        }},
        "grammatical_range_accuracy": {{
          "comments": "<sentence_level_GRA_feedback>",
          "suggestions": ["<specific_improvement>"]
        }}
      }}
    }}
  ]
}}

Provide constructive, specific feedback that helps the student improve their writing skills.
            """
        
        elif submission.submission_type == SubmissionType.CODE:
            return f"""
You are an expert code reviewer. Analyze the following code submission and provide detailed feedback.

Code to evaluate:
```
{submission.content}
```

Please evaluate the code based on these criteria:
1. Task Achievement: Does the code solve the intended problem?
2. Code Structure: Is the code well-organized and readable?
3. Best Practices: Does it follow coding conventions and best practices?
4. Error Handling: Are potential errors properly handled?

Provide feedback in this JSON format:
{{
  "overall_score": <score_out_of_10>,
  "task_achievement": {{
    "score": <score_out_of_10>,
    "comments": "<assessment_of_functionality>",
    "suggestions": ["<improvement_suggestion>"]
  }},
  "coherence_cohesion": {{
    "score": <score_out_of_10>,
    "comments": "<assessment_of_code_structure>",
    "suggestions": ["<structural_improvement>"]
  }},
  "lexical_resource": {{
    "score": <score_out_of_10>,
    "comments": "<assessment_of_variable_naming_and_clarity>",
    "suggestions": ["<naming_improvement>"]
  }},
  "grammatical_range_accuracy": {{
    "score": <score_out_of_10>,
    "comments": "<assessment_of_syntax_and_best_practices>",
    "suggestions": ["<syntax_improvement>"]
  }},
  "sentence_feedback": [
    {{
      "sentence_id": "line_001",
      "text": "<original_code_line>",
      "improved_text": "<improved_code_line>",
      "feedback_details": {{
        "task_achievement": {{"comments": "<line_level_functionality>", "suggestions": ["<improvement>"]}},
        "coherence_cohesion": {{"comments": "<line_level_structure>", "suggestions": ["<improvement>"]}},
        "lexical_resource": {{"comments": "<line_level_clarity>", "suggestions": ["<improvement>"]}},
        "grammatical_range_accuracy": {{"comments": "<line_level_syntax>", "suggestions": ["<improvement>"]}}
      }}
    }}
  ]
}}
            """
        
        else:  # Default for other submission types
            return f"""
Analyze this {submission.submission_type.value} submission and provide detailed feedback:

Content:
{submission.content}

Provide structured feedback in JSON format with overall scores and detailed sentence-level analysis.
Format your response as a JSON object with overall scores, criteria-based assessment, and sentence-by-sentence feedback.
            """
    
    async def generate_feedback(self, submission: Submission) -> Feedback:
        """Generate feedback for a submission using LLM"""
        try:
            session = await self._get_session()
            
            # Build the request payload
            payload = {
                "model": "gpt-3.5-turbo",  # or configurable model
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert evaluator providing constructive feedback on student submissions. Always provide specific, actionable suggestions."
                    },
                    {
                        "role": "user",
                        "content": self._build_prompt(submission)
                    }
                ],
                "temperature": 0.7,
                "max_tokens": 1000
            }
            
            # Make the API call
            async with session.post(f"{self.base_url}/chat/completions", json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_llm_response(data, submission)
                elif response.status == 429:
                    raise LLMServiceUnavailableError("Rate limit exceeded")
                elif response.status >= 500:
                    raise LLMServiceUnavailableError("LLM service temporarily unavailable")
                else:
                    error_text = await response.text()
                    raise LLMGatewayError(f"API request failed: {response.status} - {error_text}")
                    
        except aiohttp.ClientError as e:
            logger.error(f"HTTP client error: {str(e)}")
            raise LLMServiceUnavailableError(f"Network error: {str(e)}")
        except asyncio.TimeoutError:
            logger.error("Request timeout")
            raise LLMServiceUnavailableError("Request timeout")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise LLMGatewayError(f"Unexpected error: {str(e)}")
    
    def _parse_llm_response(self, response_data: Dict[str, Any], submission: Submission) -> Feedback:
        """Parse LLM response and create Feedback entity"""
        try:
            # Extract the generated text
            content = response_data["choices"][0]["message"]["content"]
            
            # Parse JSON response from LLM
            try:
                feedback_json = json.loads(content)
                detailed_feedback = self._parse_detailed_feedback(feedback_json)
            except json.JSONDecodeError:
                # Fallback: create basic feedback if JSON parsing fails
                logger.warning("Failed to parse JSON from LLM response, creating basic feedback")
                detailed_feedback = self._create_fallback_feedback(content)
            
            return Feedback(
                submission_id=submission.id,
                user_id=submission.user_id,
                generated_at=datetime.utcnow(),
                llm_model=response_data.get("model", "unknown"),
                feedback=detailed_feedback,
                raw_llm_output=content,
                processing_time=response_data.get("usage", {}).get("total_tokens", 0) / 1000.0,  # Rough estimation
                status=FeedbackStatus.COMPLETED,
                metadata={
                    "tokens_used": response_data.get("usage", {}).get("total_tokens", 0),
                    "submission_type": submission.submission_type.value,
                    "confidence_score": 0.85  # Default confidence
                }
            )
            
        except KeyError as e:
            logger.error(f"Failed to parse LLM response: missing key {str(e)}")
            raise LLMGatewayError(f"Invalid response format: missing {str(e)}")
        except Exception as e:
            logger.error(f"Failed to parse LLM response: {str(e)}")
            raise LLMGatewayError(f"Response parsing error: {str(e)}")
    
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
            
            async with session.post(f"{self.base_url}/chat/completions", json=payload) as response:
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
    
    async def is_healthy(self) -> bool:
        """Check if LLM service is healthy"""
        try:
            session = await self._get_session()
            
            # Simple health check payload
            payload = {
                "model": "gpt-3.5-turbo",
                "messages": [{"role": "user", "content": "Hello"}],
                "max_tokens": 5
            }
            
            async with session.post(f"{self.base_url}/chat/completions", json=payload) as response:
                return response.status == 200
                
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return False