import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.email import EmailSender
from dotenv import load_dotenv
import logging

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO)

def test_email():
    # Crear instancia del enviador de emails
    email_sender = EmailSender()

    # Destinatarios (reemplaza con tu email)
    recipients = [os.getenv('TEST_EMAIL_RECIPIENT', 'tu@email.com')]

    # Asunto
    subject = 'Test Sistema Notificaciones Shopify Sync'

    # Contenido HTML
    html_content = """
    <html>
        <body>
            <h1>Test del Sistema de Notificaciones</h1>
            <p>Este es un email de prueba del sistema de notificaciones de Shopify Sync.</p>
            <h2>Pruebas realizadas:</h2>
            <ul>
                <li>Configuración SMTP</li>
                <li>Envío de email</li>
                <li>Formato HTML</li>
                <li>Sistema de logging</li>
            </ul>
            <p style="color: blue;">Si puedes ver este mensaje en azul, el formato HTML funciona correctamente.</p>
        </body>
    </html>
    """

    # Contenido texto plano
    text_content = """
    Test del Sistema de Notificaciones
    
    Este es un email de prueba del sistema de notificaciones de Shopify Sync.
    
    Pruebas realizadas:
    - Configuración SMTP
    - Envío de email
    - Formato HTML
    - Sistema de logging
    """

    # Enviar email
    result = email_sender.send_email(
        subject=subject,
        recipients=recipients,
        html_content=html_content,
        text_content=text_content
    )

    if result:
        print("✅ Email enviado correctamente")
    else:
        print("❌ Error al enviar el email")

if __name__ == "__main__":
    test_email()