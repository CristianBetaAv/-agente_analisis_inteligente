"""
Servicio para interactuar con Azure Cosmos DB
Adaptado para oportunidades de Dynamics 365
"""
from __future__ import annotations
import os
import logging
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any


class CosmosDBService:
    """
    Servicio para guardar y consultar análisis en Cosmos DB
    
    Database: opportunity-analysis
    Container: analysis-records
    Partition Key: /opportunity_id
    """
    
    def __init__(self):
        """
        Inicializa el servicio de Cosmos DB usando COSMOS_ENDPOINT y COSMOS_KEY
        """
        try:
            # Obtener configuración desde variables de entorno
            self.endpoint = os.getenv("COSMOS_ENDPOINT")
            self.key = os.getenv("COSMOS_KEY")
            self.database_name = os.getenv("COSMOS_DATABASE_NAME", "opportunity-analysis")
            self.container_name = os.getenv("COSMOS_CONTAINER_NAME", "analysis-records")
            
            if not self.endpoint or not self.key:
                raise ValueError(
                    "COSMOS_ENDPOINT y COSMOS_KEY son requeridos"
                )
            
            logging.info(f"🔄 Inicializando CosmosClient...")
            logging.info(f"   Endpoint: {self.endpoint}")
            logging.info(f"   Database: {self.database_name}")
            logging.info(f"   Container: {self.container_name}")
            
            # Import dinámico para evitar errores de carga si la librería no está instalada
            from azure.cosmos import CosmosClient, PartitionKey, exceptions
            from azure.cosmos.database import DatabaseProxy
            from azure.cosmos.container import ContainerProxy
            
            # Inicializar cliente con endpoint y key
            self.client = CosmosClient(url=self.endpoint, credential=self.key)
            
            # Referencias (se crean bajo demanda)
            self._database: Optional[DatabaseProxy] = None
            self._container: Optional[ContainerProxy] = None
            
            logging.info(f"✅ CosmosDBService inicializado")
            
        except ImportError as ie:
            logging.error(f"❌ No se encontró la librería azure-cosmos: {ie}")
            raise
    
    @property
    def database(self) -> DatabaseProxy:
        """Obtiene o crea la base de datos"""
        if self._database is None:
            try:
                self._database = self.client.get_database_client(self.database_name)
                logging.info(f"📊 Base de datos '{self.database_name}' lista")
            except Exception as e:
                logging.error(f"❌ Error accediendo a base de datos: {str(e)}")
                raise
        return self._database
    
    @property
    def container(self) -> ContainerProxy:
        """Obtiene el contenedor"""
        if self._container is None:
            try:
                self._container = self.database.get_container_client(self.container_name)
                logging.info(f"📦 Contenedor '{self.container_name}' listo")
            except Exception as e:
                logging.error(f"❌ Error accediendo a contenedor: {str(e)}")
                raise
        return self._container
    
    def save_analysis(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Guarda un análisis en Cosmos DB
        
        Args:
            record: Diccionario con los datos del análisis
            
        Returns:
            El item creado o None si falla
        """
        try:
            logging.info(f"💾 Guardando análisis en Cosmos DB...")
            
            # Asegurar que tiene un id
            if "id" not in record:
                record["id"] = str(uuid.uuid4())
            
            # Asegurar partition key (opportunity_id)
            if "opportunity_id" not in record:
                record["opportunity_id"] = record.get("id", str(uuid.uuid4()))
            
            # Agregar timestamp si no existe
            if "processed_at" not in record:
                record["processed_at"] = datetime.utcnow().isoformat()
            
            # Guardar en Cosmos DB
            created_item = self.container.create_item(body=record)
            
            logging.info(f"✅ Análisis guardado en Cosmos DB: {record['id']}")
            return created_item
            
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"❌ Error HTTP de Cosmos DB: {e.status_code} - {e.message}")
            return None
        except Exception as e:
            logging.error(f"❌ Error guardando en Cosmos DB: {str(e)}")
            return None
    
    def get_analysis_by_opportunity(self, opportunity_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene el análisis más reciente para una oportunidad
        
        Args:
            opportunity_id: ID de la oportunidad
            
        Returns:
            Diccionario con el análisis o None
        """
        try:
            query = "SELECT * FROM c WHERE c.opportunity_id = @opportunity_id ORDER BY c.processed_at DESC"
            parameters = [{"name": "@opportunity_id", "value": opportunity_id}]
            
            items = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            if items:
                logging.info(f"📊 Análisis encontrado para oportunidad {opportunity_id}")
                return items[0]
            else:
                logging.info(f"ℹ️ No se encontró análisis para oportunidad {opportunity_id}")
                return None
            
        except Exception as e:
            logging.error(f"❌ Error consultando Cosmos DB: {str(e)}")
            return None
                
    def get_analysis_by_id(self, analysis_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene un análisis por su ID
        
        Args:
            analysis_id: ID del análisis
            
        Returns:
            Diccionario con el análisis o None
        """
        try:
            query = "SELECT * FROM c WHERE c.id = @id"
            parameters = [{"name": "@id", "value": analysis_id}]
            
            items = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            if items:
                logging.info(f"📊 Análisis encontrado con ID {analysis_id}")
                return items[0]
            else:
                logging.info(f"ℹ️ No se encontró análisis con ID {analysis_id}")
                return None
                
        except Exception as e:
            logging.error(f"❌ Error consultando Cosmos DB por ID: {str(e)}")
            return None
    
    def get_recent_analyses(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Obtiene los análisis más recientes
        
        Args:
            limit: Número máximo de resultados
            
        Returns:
            Lista de análisis
        """
        try:
            query = f"SELECT * FROM c ORDER BY c.processed_at DESC OFFSET 0 LIMIT {limit}"
            items = list(self.container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            
            logging.info(f"📊 {len(items)} análisis recientes obtenidos")
            return items
            
        except Exception as e:
            logging.error(f"❌ Error consultando análisis recientes: {str(e)}")
            return []
    
    def get_analyses_by_tower(self, tower_name: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Obtiene análisis que requieren una torre específica
        
        Args:
            tower_name: Nombre de la torre
            limit: Número máximo de resultados
            
        Returns:
            Lista de análisis
        """
        try:
            query = f"""
            SELECT * FROM c 
            WHERE ARRAY_CONTAINS(c.analysis.required_towers, @tower_name)
            ORDER BY c.processed_at DESC 
            OFFSET 0 LIMIT {limit}
            """
            parameters = [{"name": "@tower_name", "value": tower_name}]
            
            items = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            logging.info(f"📊 {len(items)} análisis encontrados para torre '{tower_name}'")
            return items
            
        except Exception as e:
            logging.error(f"❌ Error consultando por torre: {str(e)}")
            return []
