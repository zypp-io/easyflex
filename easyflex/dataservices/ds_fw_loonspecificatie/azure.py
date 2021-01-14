from azure.storage.blob import BlobServiceClient, ContainerClient
import os
import logging


def create_blob_client():
    """
    Returns Azure blob service client based on environment credentials
    -------
    """
    name, key = os.environ.get("ls_blob_account_name"), os.environ.get("ls_blob_account_key")
    connect_str = f"DefaultEndpointsProtocol=https;AccountName={name};AccountKey={key}"
    blob_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = ContainerClient.from_connection_string(
        connect_str, os.environ.get("ls_blob_container_name")
    )
    return blob_client, container_client


def upload_to_blob(file_path, filename) -> None:
    """
    Parameters
    ----------
    file_path: file_path of the XML file to be uploaded to the blob storage
    filename: the filename of the XML.
    Returns None. Uploads the XML file to the blob storage.
    -------
    """
    blob_service_client, _ = create_blob_client()
    blob_client = blob_service_client.get_blob_client(
        container=os.environ.get("ls_blob_container_name"),
        blob=filename,
    )
    logging.info(f"start uploading blob {filename}...")
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info(f"finished uploading blob {filename}!")


def download_from_blob(path, blob_name):
    _, container_client = create_blob_client()
    with open(os.path.join(path, blob_name), "wb") as my_blob:
        download_stream = container_client.download_blob(blob_name)
        my_blob.write(download_stream.readall())
