import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

class EmailSender:
    def __init__(self):
        self.smtp_host = os.getenv('SMTP_HOST')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_user = os.getenv('SMTP_USER')
        self.smtp_password = os.getenv('SMTP_PASSWORD')

    def send_email(self, subject, recipients, html_content, text_content=None):
        """
        Envía un email
        :param subject: Asunto del correo
        :param recipients: Lista de destinatarios
        :param html_content: Contenido HTML del correo
        :param text_content: Contenido texto plano (opcional)
        """
        try:
            # Crear mensaje
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.smtp_user
            msg['To'] = ', '.join(recipients)

            # Añadir versión texto plano si existe
            if text_content:
                msg.attach(MIMEText(text_content, 'plain'))
            
            # Añadir versión HTML
            msg.attach(MIMEText(html_content, 'html'))

            # Conectar y enviar
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)

            logger.info(f"Email enviado correctamente a {', '.join(recipients)}")
            return True

        except Exception as e:
            logger.error(f"Error al enviar email: {str(e)}")
            return False