import os
import json
from urllib.parse import unquote_plus
import logging
import boto3


print("Loading function")
logger = logging.getLogger()
logger.setLevel("INFO")
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
reckognition = boto3.client("rekognition")

table = dynamodb.Table(os.getenv("table"))


def lambda_handler(event, context):
    logger.info(json.dumps(event, indent=2))
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])

    # Récupération de l'utilisateur et de l'UUID de la tâche
    username, post_id = key.split("/")[:2]

    pk = f"USER#{username}"
    sk = f"POST#{post_id}"

    # Ajout des tags user et task_uuid
    try:
        s3.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={
                "TagSet": [
                    {"Key": "PK", "Value": pk},
                    {"Key": "SK", "Value": sk},
                ]
            },
        )
        logger.info("Tags added to %s", key)
    except Exception as e:
        logger.error("Failed to tag object: %s", e)

    # Appel à reckognition
    label_data = reckognition.detect_labels(
        Image={"S3Object": {"Bucket": bucket, "Name": key}},
        MaxLabels=5,
        MinConfidence=0.75,
    )

    logger.info("Labels data : %s", label_data)

    # Récupération des résultats des labels
    labels = [label["Name"] for label in label_data["Labels"]]
    logger.info("Labels detected : %s", labels)
    # Mise à jour de la table dynamodb
    try:
        logger.info("Saving image and labels for post %s", post_id)
        table.update_item(
            Key={
                "PK": pk,
                "SK": sk,
            },
            AttributeUpdates={
                "image": {"Value": f"s3://{bucket}/{key}", "Action": "PUT"},
                "labels": {"Value": labels, "Action": "PUT"},
            },
            ReturnValues="UPDATED_NEW",
        )

    except Exception as e:
        logger.error("Unable to update post. Error %s", e)
        raise e
