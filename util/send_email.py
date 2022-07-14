# Import smtplib for the actual sending function
import smtplib
import ssl
from email.message import EmailMessage
from dataclasses import dataclass
from inputs import pwd


@dataclass
class SendEmail:

    subject: str
    content: str

    def _compose_email(self):
        msg = EmailMessage()
        msg.set_content(self.content)
        msg['Subject'] = self.subject
        msg['From'] = pwd.EMAIL
        msg['To'] = pwd.EMAIL

        return msg

    def send(self):
        context = ssl.create_default_context()

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(pwd.EMAIL, pwd.EMAIL_PASSWORD)
            server.send_message(self._compose_email())


if __name__ == '__main__':
    SendEmail(subject=f'Daily job started for 2022-02-01',
              content='This job will population tables: \n     --`yahoo_fundamental`\n').send()