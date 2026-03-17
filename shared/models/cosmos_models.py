"""
Modelos de datos para Cosmos DB
Definición de estructuras para el análisis de oportunidades
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class TeamRecommendation(BaseModel):
    """Recomendación de equipo/torre para un proyecto"""
    
    tower: str = Field(..., description="Nombre de la torre")
    team_name: Optional[str] = Field(None, description="Nombre del equipo")
    team_lead: Optional[str] = Field(None, description="Líder del equipo")
    team_lead_email: Optional[str] = Field(None, description="Email del líder")
    role_in_project: Optional[str] = Field(None, description="Rol en el proyecto")
    justification: Optional[str] = Field(None, description="Justificación de la recomendación")
    estimated_effort: Optional[str] = Field(None, description="Esfuerzo estimado")
    priority: Optional[str] = Field("Media", description="Prioridad: Alta, Media, Baja")
    skills_required: Optional[List[str]] = Field(default_factory=list, description="Habilidades requeridas")
    
    class Config:
        extra = "allow"


class Risk(BaseModel):
    """Riesgo identificado en el análisis"""
    
    risk: str = Field(..., description="Descripción del riesgo")
    impact: Optional[str] = Field("Medio", description="Impacto: Alto, Medio, Bajo")
    probability: Optional[str] = Field("Media", description="Probabilidad: Alta, Media, Baja")
    mitigation: Optional[str] = Field(None, description="Estrategia de mitigación")
    category: Optional[str] = Field(None, description="Categoría del riesgo")
    
    class Config:
        extra = "allow"


class EffortEstimate(BaseModel):
    """Estimación de esfuerzo del proyecto"""
    
    total_hours: Optional[int] = Field(None, description="Horas totales estimadas")
    total_days: Optional[int] = Field(None, description="Días totales estimados")
    team_size: Optional[int] = Field(None, description="Tamaño del equipo sugerido")
    duration_weeks: Optional[int] = Field(None, description="Duración en semanas")
    breakdown: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Desglose por fase/torre")
    
    class Config:
        extra = "allow"


class AnalysisRecord(BaseModel):
    """
    Registro completo de análisis guardado en Cosmos DB
    
    Partition Key: opportunity_id (o work_item_id para compatibilidad)
    """
    
    # Identificadores
    id: str = Field(..., description="ID único del registro")
    opportunity_id: Optional[str] = Field(None, description="ID de la oportunidad de Dynamics")
    work_item_id: Optional[str] = Field(None, description="ID del Work Item (legacy)")
    work_item_url: Optional[str] = Field(None, description="URL del Work Item (legacy)")
    
    # Timestamps
    processed_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    status: str = Field("completed", description="Estado del procesamiento")
    
    # Análisis principal
    executive_summary: Optional[str] = Field(None, description="Resumen ejecutivo")
    key_points: Optional[List[str]] = Field(default_factory=list, description="Puntos clave")
    key_requirements: Optional[List[str]] = Field(default_factory=list, description="Requerimientos clave")
    technical_assessment: Optional[str] = Field(None, description="Evaluación técnica")
    
    # Tecnologías
    technology_stack: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Stack tecnológico")
    technology_recommendations: Optional[List[str]] = Field(default_factory=list)
    architecture_suggestions: Optional[List[str]] = Field(default_factory=list)
    
    # Torres y equipos
    required_towers: Optional[List[str]] = Field(default_factory=list, description="Torres requeridas")
    team_recommendations: Optional[List[TeamRecommendation]] = Field(default_factory=list)
    
    # Riesgos
    risks: Optional[List[Risk]] = Field(default_factory=list, description="Riesgos identificados")
    overall_risk_level: Optional[str] = Field("Medio", description="Nivel de riesgo general")
    
    # Estimaciones
    effort_estimate: Optional[Dict[str, Any]] = Field(default_factory=dict)
    timeline_estimate: Optional[Dict[str, Any]] = Field(default_factory=dict)
    
    # Confianza y recomendaciones
    analysis_confidence: Optional[float] = Field(0.0, description="Confianza del análisis (0-1)")
    recommendations: Optional[List[str]] = Field(default_factory=list)
    next_steps: Optional[List[str]] = Field(default_factory=list)
    clarification_questions: Optional[List[str]] = Field(default_factory=list)
    
    # Datos originales
    original_description: Optional[str] = Field(None, description="Descripción original")
    attachments_processed: Optional[List[str]] = Field(default_factory=list)
    extracted_text_length: Optional[int] = Field(0)
    
    # Metadata
    organization: Optional[str] = Field(None)
    project: Optional[str] = Field(None)
    source: Optional[str] = Field("power_automate", description="Fuente del análisis")
    event_type: Optional[str] = Field(None, description="Tipo de evento (Create, Update)")
    
    class Config:
        extra = "allow"
