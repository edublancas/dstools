"""
CLI to run commands, capture output and send notifications if it fails
(email includes captured output)
"""
import argparse
import subprocess
import smtplib
from email.message import EmailMessage

email_from = None
email_to = None
smtp_server = None


def run(command):
    output_all = []
    process = subprocess.Popen(command,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               shell=True)
    while True:
        output = process.stdout.readline().decode('utf-8')

        if output == '' and process.poll() is not None:
            break

        if output:
            output_all.append(output)
            print(output.strip())

    return_code = process.poll()

    if return_code:
        # error happened
        print('Script finished with abnormal code status: %i' % return_code)

        msg = EmailMessage()
        msg.set_content(''.join(output_all))

        msg['Subject'] = 'Error running "%s"' % command
        msg['From'] = email_from
        msg['To'] = email_to

        s = smtplib.SMTP(smtp_server)
        s.send_message(msg)
        s.quit()

    else:
        print('Script ended with zero code status')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('command', type=str)
    args = parser.parse_args()
    run(args.command)
