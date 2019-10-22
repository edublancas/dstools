from dstools import Env

import warnings
import smtplib
from email.message import EmailMessage


def send_email(subject, body):
    env = Env()

    msg = EmailMessage()
    msg.set_content(body)

    try:
        sender = env.notifications.email.sender
    except KeyError:
        sender = None

    try:
        to = env.notifications.email.to
    except KeyError:
        to = None

    if sender is not None and to is not None:
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = env.notifications.email.to

        s = smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()
    else:
        if sender is None:
            warnings.warn('Cannot send email, could not load sender')

        if to is None:
            warnings.warn('Cannot send email, could not load recipient')
