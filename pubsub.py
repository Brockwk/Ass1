from google.cloud import pubsub_v1
import smtplib
from email.mime.text import MIMEText
import os

# Set Google Cloud credentials (upload the key file and set path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/content/recommded-bank-order-92a2a70e7c05.json"

# Google Cloud Project and Subscription details
PROJECT_ID = "recommded-bank-order"
SUBSCRIPTION_NAME = "file-upload-subscription"

# Email Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "swapnilyard@gmail.com"  # Your email address
EMAIL_PASS = "Forgot@9023"     # Your email password (use app password for Gmail)
TO_EMAIL = "pandurangshete6@gmail.com"     # Recipient's email address

def send_email(file_name, bucket_name):
    """
    Sends an email notification with the file details.
    """
    subject = "File Uploaded to Cloud Storage"
    body = f"A new file '{file_name}' was uploaded to the bucket '{bucket_name}'."
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_USER
    msg['To'] = TO_EMAIL

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, TO_EMAIL, msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

def callback(message):
    """
    Callback function to process incoming Pub/Sub messages.
    """
    try:
        print(f"Received message: {message.data}")
        attributes = message.attributes
        file_name = attributes.get("objectId")
        bucket_name = attributes.get("bucketId")

        if file_name and bucket_name:
            send_email(file_name, bucket_name)
        else:
            print("Message does not contain required attributes.")

        message.ack()  # Acknowledge the message after successful processing
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    """
    Main function to listen to Pub/Sub messages and trigger email notifications.
    """
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    print(f"Listening for messages on {subscription_path}...")

    future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        future.result()  # Keep the subscription open and listening
    except KeyboardInterrupt:
        future.cancel()
        print("\nStopped listening for messages.")

if __name__ == "__main__":
    main()
