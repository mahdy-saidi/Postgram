#################################################################################################
##                                                                                             ##
##                                 NE PAS TOUCHER CETTE PARTIE                                 ##
##                                                                                             ##
## 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 ##
import boto3
from botocore.config import Config
import os
import mimetypes
import uuid
from dotenv import load_dotenv
from typing import Union
import logging
from fastapi import FastAPI, Request, status, Header, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from getSignedUrl import getSignedUrl

load_dotenv()

app = FastAPI()
logger = logging.getLogger("uvicorn")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logger.error(f"{request}: {exc_str}")
    content = {"status_code": 10422, "message": exc_str, "data": None}
    return JSONResponse(
        content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
    )


class Post(BaseModel):
    title: str
    body: str


my_config = Config(
    region_name="us-east-1",
    signature_version="v4",
)

dynamodb = boto3.resource("dynamodb", config=my_config)
table = dynamodb.Table(os.getenv("DYNAMO_TABLE"))
s3_client = boto3.client("s3", config=boto3.session.Config(signature_version="s3v4"))
bucket = os.getenv("BUCKET")

## ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ##
##                                                                                                ##
####################################################################################################


@app.post("/posts")
async def post_a_post(
    post: Post,
    authorization: str | None = Header(default=None),
):
    """
    Poste un post ! Les informations du poste sont dans post.title, post.body
    et le user dans authorization
    """

    if authorization is None:
        raise HTTPException(
            status_code=401,
            detail="Authorization header is missing",
        )

    logger.info("title : %s", post.title)
    logger.info("body : %s", post.body)
    logger.info("user : %s", authorization)

    title = post.title
    body = post.body
    post_id = str(uuid.uuid4())

    pk = f"USER#{authorization}"  # Partition key : utilisateur
    sk = f"POST#{post_id}"  # Sort key : ID du post

    try:
        res = table.put_item(
            Item={
                "PK": pk,
                "SK": sk,
                "title": title,
                "body": body,
            }
        )
        logger.info("DynamoDB response: %s", res)
    except Exception as e:
        logger.error("Error saving post: %s", e)
        raise HTTPException(
            status_code=500, detail="Server error while saving the post"
        ) from e

    # Doit retourner le résultat de la requête la table dynamodb
    return res


@app.get("/posts")
async def get_all_posts(user: Union[str, None] = None):
    """
    Récupère tout les postes.
    - Si un user est présent dans le requête, récupère uniquement les siens
    - Si aucun user n'est présent, récupère TOUS les postes de la table !!
    """
    if user:
        logger.info("Récupération des postes de : %s", user)
        posts = table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={
                ":pk": f"USER#{user}",
            },
        )
    else:
        logger.info("Récupération de tous les postes")
        posts = table.scan()

    res = []

    for item in posts.get("Items", []):

        username = item.get("PK", "")
        post_id = item.get("SK", "")
        title = item.get("title ", "")
        body = item.get("body", "")

        if item.get("image"):
            folder, subfolder, filename = item.get("image").split('/')[-3:]
            filetype, _ = mimetypes.guess_type(filename)
            if not filetype:
                filetype = "application/octet-stream"
            image = getSignedUrl(filename, filetype, subfolder, folder)
        else:
            image = ""
        labels = item.get("labels", [])

        formatted_item = {
            "user": username,
            "id": post_id,
            "title": title,
            "body": body,
            "image": image,
            "labels": labels,
        }
        res.append(formatted_item)

    return res


@app.delete("/posts/{post_id}")
async def delete_post(
    post_id: str,
    authorization: str | None = Header(default=None),
):
    # Doit retourner le résultat de la requête la table dynamodb
    logger.info("post id : %s", post_id)
    logger.info("user: %s", authorization)
    # Récupération des infos du poste
    pk = "USER#" + authorization
    sk = "POST#" + post_id
    post = table.query(
        KeyConditionExpression="PK = :pk AND SK = :sk",
        ExpressionAttributeValues={
            ":pk": pk,
            ":sk": sk,
        },
    )
    # S'il y a une image on la supprime de S3
    if post.get("image"):
        folder_name = f"{authorization}/{post_id}/"
        try:
            logger.info("Deleting S3 folder %s", folder_name)
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=folder_name,
            )
            keys = [{"Key": k["Key"]} for k in response.get("Contents", [])]
            if keys:
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": keys},
                )
        except Exception as e:
            logger.error("Error deleting %s: %s", folder_name, e)

    # Suppression de la ligne dans la base dynamodb
    try:
        item = table.delete_item(
            Key={
                "PK": pk,
                "SK": sk,
            }
        )
    except Exception as e:
        logger.error("Error deleting post: %s", e)
        raise e

    # Retourne le résultat de la requête de suppression
    return item


#################################################################################################
##                                                                                             ##
##                                 NE PAS TOUCHER CETTE PARTIE                                 ##
##                                                                                             ##
## 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 ##
@app.get("/signedUrlPut")
async def get_signed_url_put(
    filename: str,
    filetype: str,
    postId: str,
    authorization: str | None = Header(default=None),
):
    return getSignedUrl(filename, filetype, postId, authorization)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="debug")

## ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ##
##                                                                                                ##
####################################################################################################
