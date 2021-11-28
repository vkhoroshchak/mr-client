import os
import smtplib
import ssl
from email.header import Header
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import Template


class EmailNotifier:
    __sender_email = os.getenv("NO_REPLY_EMAIL")
    __sender_password = os.getenv("NO_REPLY_EMAIL_PASSWORD")

    def __init__(self, email_to):
        self.email_to = email_to

    def send_email(self, email_template, additional_info):
        with open(Path(email_template["template_path"]), encoding="UTF-8") as f:
            email_body = Template(f.read()).render(additional_info)

        context = ssl.create_default_context()

        with smtplib.SMTP_SSL(os.getenv("SMTP_HOST"), os.getenv("SMTP_PORT"), context=context) as server:
            message = MIMEMultipart("related")

            message["From"] = Header(email_template["from"])
            message["To"] = Header(self.email_to)
            message["Subject"] = Header(email_template["subject"])

            message.attach(MIMEText(email_body, "html", "utf-8"))

            with open(Path("templates/email_templates/img/21f9b932ec8dc4b98dfcabb03cff91bd.png"),
                      'rb') as map_reduce_logo:
                img_logo = MIMEImage(map_reduce_logo.read(), 'svg')
                img_logo.add_header('Content-Id', '<map_reduce_logo>')
                message.attach(img_logo)

            server.login(self.__sender_email, self.__sender_password)
            server.sendmail(self.__sender_email, self.email_to, message.as_string())
