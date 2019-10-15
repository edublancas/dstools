from dstools import Env

import smtplib
from email.message import EmailMessage


def send_email(subject, body):
    env = Env()

    msg = EmailMessage()
    msg.set_content(body)

    msg['Subject'] = subject
    msg['From'] = env.notifications.email.sender
    msg['To'] = env.notifications.email.to

    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()
