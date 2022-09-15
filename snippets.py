# IMPORT

import json
from typing import Any
from loguru import logger
import boto3
import pandas as pd
from io import StringIO

#


def s3_get_privacy_mode():
    return ["private", "public-read", "public-read-write", "authenticated-read"]


def s3_get_locations():
    return [
        "af-south-1",
        "ap-east-1",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-northeast-3",
        "ap-south-1",
        "ap-southeast-1",
        "ap-southeast-2",
        "ca-central-1",
        "cn-north-1",
        "cn-northwest-1",
        "eu-central-1",
        "eu-north-1",
        "eu-south-1",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "me-south-1",
        "sa-east-1",
        "us-east-2",
        "us-gov-east-1",
        "us-gov-west-1",
        "us-west-1",
        "us-west-2",
    ]


# S3


def s3_lista_bucket():
    """Lista i bucket"""
    client = boto3.client("s3")
    return [b['Name'] for b in client.list_buckets()["Buckets"]]


def s3_read_obj(bucket: str, nomefile: str):
    client = boto3.client("s3")
    return client.get_object(Bucket=bucket, Key=nomefile)["Body"]


def s3_read_html(bucket: str, filename: str):
    client = boto3.client("s3")
    logger.debug("Lettura del file html S3")
    file = client.get_object(Bucket=bucket, Key=filename)["Body"]
    return file.read().decode("utf-8")


def s3_lista_oggetti(bucket: str):
    """Lista i files contenuti in un bucket"""
    s3 = boto3.resource("s3")
    return [f.key for f in s3.Bucket(bucket).objects.all()]


def s3_cancella_bucket(bucket: str):
    """Cancella il bucket"""
    s3 = boto3.resource("s3")
    s3.Bucket(bucket).delete()
    logger.debug("Bucket cancellato")
    return True


def s3_nuke_bucket(bucket: str):
    """Cancella i contenuti di bucket"""
    s3 = boto3.resource("s3")
    s3.Bucket(bucket).objects.all().delete()
    logger.debug("Bucket nuclearizzato")
    return True


def s3_crea_bucket(
    bucket_name: str, mode: str = "private", location: str = "eu-central-1"
) -> bool:
    """Crea un bucket, se inesistente. restituisce False se il bucket esiste"""
    client = boto3.client("s3")
    lista_bucket = [""]
    try:
        lista_bucket = [b_info["Name"] for b_info in client.list_buckets()["Buckets"]]
    except Exception:
        lista_bucket = []

    if bucket_name in lista_bucket:
        logger.debug("Bucket già esistente.")
        return False
    else:
        logger.debug(
            "Bucket non esistente. Procedo alla creazione del bucket {}".format(
                bucket_name
            )
        )

        info_bucket = client.create_bucket(
            # 'private','public-read','public-read-write','authenticated-read'
            ACL=mode,  # type: ignore
            Bucket=bucket_name,
            CreateBucketConfiguration={
                # 'af-south-1','ap-east-1','ap-northeast-1','ap-northeast-2','ap-northeast-3','ap-south-1','ap-southeast-1','ap-southeast-2','ca-central-1','cn-north-1','cn-northwest-1','EU','eu-central-1','eu-north-1','eu-south-1','eu-west-1','eu-west-2','eu-west-3','me-south-1','sa-east-1','us-east-2','us-gov-east-1','us-gov-west-1','us-west-1','us-west-2'
                "LocationConstraint": location  # type: ignore
            },
        )
        return True


def s3_upload_csv(
    dataframe: pd.DataFrame, name: str, bucket: str, metas: dict[str, str] = {}
):
    """usa pandas qua per prendere il csv e spostarlo nel bucket senza passare da tempfile"""
    client = boto3.client("s3")
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    client.put_object(
        Bucket=bucket, Key=name, Body=csv_buffer.getvalue(), Metadata={**metas}
    )
    logger.debug(f"{name} caricato sul bucket {bucket}")
    return True


def s3_upload_json(metadati: dict, name: str, bucket: str, metas: dict[str, str] = {}):
    """update di un file json a partire da un dict"""
    client = boto3.client("s3")
    bytes = json.dumps(metadati).encode("utf-8")
    client.put_object(Bucket=bucket, Key=name, Body=bytes, Metadata={**metas})
    logger.debug(f"{name} caricato sul bucket {bucket}")
    return True


def s3_upload_stream(
    buffer: StringIO, name: str, bucket: str, metas: dict[str, str] = {}
):
    """update di un file json a partire da un dict"""
    client = boto3.client("s3")
    client.put_object(
        Bucket=bucket, Key=name, Body=buffer.getvalue(), Metadata={**metas}
    )
    logger.debug(f"{name} caricato sul bucket {bucket}")
    return True


def s3_upload_file(filename: str, name: str, bucket: str, metas: dict[str, str] = {}):
    """upload di un file"""
    client = boto3.client("s3")
    with open(filename, "r") as f:
        bytes = f.read().encode("utf-8")
    client.put_object(Bucket=bucket, Key=name, Body=bytes, Metadata={**metas})
    logger.debug(f"{name} caricato sul bucket {bucket}")
    return True


# SQS


def sqs_lunghezza_coda(url_coda: str):
    """lunghezza della coda sqs"""
    client = boto3.client("sqs")
    lung = client.get_queue_attributes(
        QueueUrl=url_coda, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]["ApproximateNumberOfMessages"]
    return lung


def sqs_get_url_coda(nome_coda: str):
    """partendo dal nome, recupera l'URL della coda"""
    resource = boto3.resource("sqs")
    coda = resource.get_queue_by_name(QueueName=nome_coda)
    queue_url = str(coda.url)
    return queue_url


def sqs_ricevi_msg(url_coda: str):
    """ricevi un messaggio dalla coda"""
    client = boto3.client("sqs")
    logger.debug("Estraggo un elemento dalla coda aws")
    res = client.receive_message(QueueUrl=url_coda, MaxNumberOfMessages=1,)[
        "Messages"
    ][0]
    return {"Body": res["Body"], "ReceiptHandle": res["ReceiptHandle"]}


def sqs_cancella_msg(url_coda: str, receipt_handle: Any):
    """cancella un messaggio dalla coda"""
    client = boto3.client("sqs")
    client.delete_message(QueueUrl=url_coda, ReceiptHandle=receipt_handle)
    return True

def sqs_invio_messaggio(
    url_coda: str, messaggio: str, group_id: str, attributi_msg: str = ""
):
    """Invia un msg"""
    client = boto3.client("sqs")
    client.send_message(
        QueueUrl=url_coda,
        MessageBody=messaggio,
        DelaySeconds=0,
        MessageAttributes={
            "filename": {
                "StringValue": attributi_msg,
                "DataType": "String",
            }
        },
        MessageGroupId=group_id,
    )
    return True


def sqs_crea_coda(nome_coda: str, fifo: bool = True):
    """Crea la coda SQS"""
    client = boto3.resource("sqs")
    try:
        queue = client.get_queue_by_name(QueueName=f"{nome_coda}")
        url_coda = queue.url
        logger.debug(f"URL della coda : {url_coda}")
    except:
        logger.debug("Coda non trovata. Creo la coda")
        if fifo:
            if not nome_coda.endswith(".fifo"):
                nome_coda += ".fifo"
            url_coda = client.create_queue(
                QueueName=f"{nome_coda}",
                Attributes={
                    "FifoQueue": "true",
                    "VisibilityTimeout": "30",
                    "ContentBasedDeduplication": "True",
                },
            ).url
        else:
            url_coda = client.create_queue(
                QueueName=f"{nome_coda}",
                Attributes={
                    "FifoQueue": "false",
                    "VisibilityTimeout": "30",
                    "ContentBasedDeduplication": "True",
                },
            ).url
    return url_coda

def sqs_lista_code():
    client=boto3.client('sqs')
    return client.list_queues()['QueueUrls']

def sqs_cancella_coda(queue_url):
    client.delete_queue(QueueUrl=queue_url)
    return True
    
def sqs_nuke_coda(queue_url):
    client.purge_queue(QueueUrl=queue_url)
    return True

# ECS


def shutdown(signum, frame):
    """Funzione da inserire nel file contenente il main del task.
    Nel main inserire inoltre

    signal.signal(signal.SIGTERM, shutdown)


    """
    print("Caught SIGTERM, shutting down")
    exit(0)
