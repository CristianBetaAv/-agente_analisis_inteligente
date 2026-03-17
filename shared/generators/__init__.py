"""
Generadores para el Agente de Análisis Inteligente
"""

from .adaptive_card import generate_opportunity_card
from .pdf_generator import PDFGenerator

__all__ = [
    'generate_opportunity_card',
    'PDFGenerator',
]
