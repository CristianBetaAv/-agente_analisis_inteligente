"""
Modelos de datos para el Agente de Análisis Inteligente
"""

from .opportunity import OpportunityPayload
from .analysis import (
    TeamRecommendation,
    RiskAssessment,
    BudgetEstimate,
    TimelinePhase,
    OpportunityAnalysis,
    AnalysisResponse,
    ErrorResponse
)

__all__ = [
    # Opportunity
    'OpportunityPayload',
    
    # Analysis
    'TeamRecommendation',
    'RiskAssessment', 
    'BudgetEstimate',
    'TimelinePhase',
    'OpportunityAnalysis',
    'AnalysisResponse',
    'ErrorResponse',
]
