"""
Servicio para Azure Blob Storage
Almacenamiento de PDFs y documentos generados
"""

import os
import logging
from typing import Optional
from azure.storage.blob import BlobServiceClient, ContentSettings
from datetime import datetime, timedelta


class BlobStorageService:
    """Servicio para Azure Blob Storage"""
    
    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "analysis-reports")
        
        if not self.connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING es requerido")
        
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )
        
        # Asegurar que el contenedor existe
        self._ensure_container_exists()
        
        logging.info(f"✅ BlobStorageService inicializado: {self.container_name}")
    
    def _ensure_container_exists(self):
        """Crea el contenedor si no existe"""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )
            
            if not container_client.exists():
                container_client.create_container()
                logging.info(f"✅ Contenedor creado: {self.container_name}")
            
        except Exception as e:
            logging.error(f"❌ Error creando contenedor: {str(e)}")
    
    def upload_pdf(
        self, 
        pdf_bytes: bytes, 
        blob_name: str
    ) -> Optional[str]:
        """
        Sube un PDF al Blob Storage
        
        Args:
            pdf_bytes: Contenido del PDF en bytes
            blob_name: Nombre/ruta del blob (ej: "opportunity-analysis/123/20240204.pdf")
            
        Returns:
            URL pública del PDF o None si falla
        """
        try:
            
            logging.info(f"📤 Subiendo PDF: {blob_name}")
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            # Configurar content type
            content_settings = ContentSettings(content_type='application/pdf')
            
            # Subir el archivo
            blob_client.upload_blob(
                pdf_bytes,
                overwrite=True,
                content_settings=content_settings
            )
            
            # Generar URL con SAS token (válido por 90 días)
            url = self._generate_blob_url_with_sas(blob_name, days=90)
            
            logging.info(f"✅ PDF subido: {url}")
            return url
            
        except Exception as e:
            logging.error(f"❌ Error subiendo PDF: {str(e)}")
            return None
    
    def _generate_blob_url_with_sas(self, blob_name: str, days: int = 90) -> str:
        """
        Genera URL con SAS token para acceso público
        
        Args:
            blob_name: Nombre del blob
            days: Días de validez del token
            
        Returns:
            URL completa con SAS token
        """
        try:
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            # Generar SAS token
            sas_token = generate_blob_sas(
                account_name=self.blob_service_client.account_name,
                container_name=self.container_name,
                blob_name=blob_name,
                account_key=self._get_account_key(),
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(days=days)
            )
            
            # Construir URL completa
            url = f"{blob_client.url}?{sas_token}"
            return url
            
        except Exception as e:
            logging.error(f"❌ Error generando SAS: {str(e)}")
            # Devolver URL sin SAS (solo funciona si el contenedor es público)
            return blob_client.url
    
    def _get_account_key(self) -> str:
        """Extrae la account key del connection string"""
        try:
            parts = self.connection_string.split(';')
            for part in parts:
                if part.startswith('AccountKey='):
                    return part.split('=', 1)[1]
            return ""
        except:
            return ""
    
    def download_blob(self, blob_name: str) -> Optional[bytes]:
        """
        Descarga un blob
        
        Args:
            blob_name: Nombre del blob
            
        Returns:
            Contenido del blob en bytes
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            download_stream = blob_client.download_blob()
            return download_stream.readall()
            
        except Exception as e:
            logging.error(f"❌ Error descargando blob: {str(e)}")
            return None
