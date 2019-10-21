import ssl
from smtplib import SMTP


def SendEmailNotification(SMTP_server, password, port, sender_email, receiver_email, message):
    context = ssl.create_default_context()
    with SMTP(SMTP_server, port) as server:
        server.starttls(context=context)
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)