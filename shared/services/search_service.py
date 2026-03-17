"""
Servicio para Azure AI Search
Búsqueda de equipos y conocimiento organizacional
"""

import os
import logging
from typing import List, Dict, Any


class SearchService:
    """Servicio para Azure AI Search"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.key = os.getenv("AZURE_SEARCH_KEY")
        self.index_name = os.getenv("AZURE_SEARCH_INDEX_TEAMS", "teams-index")
        
        if not self.endpoint or not self.key:
            raise ValueError("AZURE_SEARCH_ENDPOINT y AZURE_SEARCH_KEY son requeridos")
        
        try:
            # Import dinámico para evitar errores de carga si la librería no está instalada
            from azure.search.documents import SearchClient
            from azure.core.credentials import AzureKeyCredential
        except ImportError as ie:
            logging.error(f"❌ No se encontró la librería azure-search-documents: {ie}")
            raise
        
        self.client = SearchClient(
            endpoint=self.endpoint,
            index_name=self.index_name,
            credential=AzureKeyCredential(self.key)
        )
        
        logging.info(f"✅ SearchService inicializado: {self.index_name}")
    
    def search_teams(self, query: str, top: int = 10) -> List[Dict[str, Any]]:
        """
        Busca equipos relevantes basándose en una query
        
        Args:
            query: Texto de búsqueda (tecnologías, habilidades, etc)
            top: Número máximo de resultados
            
        Returns:
            Lista de equipos encontrados
        """
        try:
            logging.info(f"🔍 Buscando equipos para: {query[:100]}...")
            
            results = self.client.search(
                search_text=query,
                top=top,
                select=["id", "team_name", "tower", "team_lead", "team_lead_email", "skills", "expertise_areas", "technologies", "frameworks", "description"],
                include_total_count=True
            )
            
            teams = []
            for result in results:
                # Los campos ya vienen como arrays desde AI Search
                team = {
                    "id": result.get("id", ""),
                    "name": result.get("team_name", ""),
                    "tower": result.get("tower", ""),
                    "leader": result.get("team_lead", ""),
                    "leader_email": result.get("team_lead_email", ""),
                    "skills": result.get("skills", []) if isinstance(result.get("skills"), list) else [],
                    "expertise_areas": result.get("expertise_areas", []) if isinstance(result.get("expertise_areas"), list) else [],
                    "technologies": result.get("technologies", []) if isinstance(result.get("technologies"), list) else [],
                    "frameworks": result.get("frameworks", []) if isinstance(result.get("frameworks"), list) else [],
                    "description": result.get("description", ""),
                    "search_score": result.get("@search.score", 0.0)
                }
                teams.append(team)
            
            logging.info(f"✅ {len(teams)} equipos encontrados")
            return teams
            
        except Exception as e:
            logging.error(f"❌ Error buscando equipos: {str(e)}")
            return []
    
    def get_all_teams(self) -> List[Dict[str, Any]]:
        """
        Obtiene todos los equipos disponibles
        
        Returns:
            Lista completa de equipos
        """
        try:
            logging.info("📋 Obteniendo todos los equipos...")
            
            # Búsqueda con "*" para obtener todo
            results = self.client.search(
                search_text="*",
                select=["id", "team_name", "tower", "team_lead", "team_lead_email", "skills", "expertise_areas", "technologies", "frameworks", "description"],
                top=100
            )
            
            teams = []
            for result in results:
                # Los campos ya vienen como arrays desde AI Search
                team = {
                    "id": result.get("id", ""),
                    "name": result.get("team_name", ""),
                    "tower": result.get("tower", ""),
                    "leader": result.get("team_lead", ""),
                    "leader_email": result.get("team_lead_email", ""),
                    "skills": result.get("skills", []) if isinstance(result.get("skills"), list) else [],
                    "expertise_areas": result.get("expertise_areas", []) if isinstance(result.get("expertise_areas"), list) else [],
                    "technologies": result.get("technologies", []) if isinstance(result.get("technologies"), list) else [],
                    "frameworks": result.get("frameworks", []) if isinstance(result.get("frameworks"), list) else [],
                    "description": result.get("description", ""),
                    "search_score": result.get("@search.score", 0.0)
                }
                teams.append(team)
            
            logging.info(f"✅ {len(teams)} equipos totales")
            return teams
            
        except Exception as e:
            logging.error(f"❌ Error obteniendo equipos: {str(e)}")
            return []
    
    def search_by_skills(self, skills: List[str], top: int = 10) -> List[Dict[str, Any]]:
        """
        Busca equipos que tengan habilidades específicas
        
        Args:
            skills: Lista de habilidades requeridas
            top: Número máximo de resultados
            
        Returns:
            Lista de equipos que matchean las habilidades
        """
        # Crear query con las habilidades
        query = " OR ".join(skills)
        return self.search_teams(query, top)
