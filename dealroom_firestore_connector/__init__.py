""" Firestore connector for exception handling with Dealroom data"""
import logging
import os
import traceback
from time import sleep
from typing import Tuple

from google.cloud import firestore
from google.cloud.firestore_v1.collection import CollectionReference

from .batch import Batcher
from .helpers import error_logger
from .status_codes import ERROR, SUCCESS

# Time to sleep in seconds when a exception occurrs until retrying
EXCEPTION_SLEEP_TIME = 5


def new_connection(project: str, credentials_path: str = None):
    """Start a new connection with Firestore.
    Args:
        project (str): project id of Firestore database
        credentials_path (str): path to credentials json file
    Returns:
        [object]: Firestore db instance
    """
    try:
        if credentials_path:
            return firestore.Client.from_service_account_json(credentials_path)
        else:
            return firestore.Client(project=project)
    except Exception as identifier:
        __log_exception(5, credentials_path, identifier, True)
        return ERROR


def get(doc_ref, *args, **kwargs):
    """Retrieve a document from Firestore
    Args:
        doc_ref (object): Firestore reference to the document.
    Returns:
        [object]: Firestore document object
        -1 [int]: exception (error after retrying a second time)
    """
    try:
        return doc_ref.get(*args, **kwargs)
    except Exception as identifier:
        # log error
        __log_exception(3, doc_ref, identifier)
        # Wait before continue
        sleep(EXCEPTION_SLEEP_TIME)
        try:
            # Retry
            return doc_ref.get(*args, **kwargs)
        except Exception as identifier:
            # log error
            __log_exception(3, doc_ref, identifier, True)
            return ERROR


def set(doc_ref, *args, **kwargs):
    """Create a new document in Firestore
    Args:
        doc_ref (object): Firestore reference to the document that will be created.
    Returns:
        0 [int]: success (1st or 2nd time tried)
        -1 [int]: exception (error after retrying a second time)
    """
    try:
        doc_ref.set(*args, **kwargs, merge=True)
        return SUCCESS
    except Exception as identifier:
        # log error
        __log_exception(4, doc_ref, identifier)
        # Wait before continue
        sleep(EXCEPTION_SLEEP_TIME)
        try:
            # Retry
            doc_ref.set(*args, **kwargs, merge=True)
            return SUCCESS
        except Exception as identifier:
            # log error
            __log_exception(4, doc_ref, identifier, True)
            return ERROR


def update(doc_ref, *args, **kwargs):
    """Update a Firestore document.
    Args:
        doc_ref (object): Firestore reference to the document that will be updated.
    Returns:
        0 [int]: success (1st or 2nd time tried)
        -1 [int]: exception (error after retrying a second time)
    """
    try:
        doc_ref.update(*args, **kwargs)
        return SUCCESS
    except Exception as identifier:
        # log error
        __log_exception(2, doc_ref, identifier)
        # Wait before continue
        sleep(EXCEPTION_SLEEP_TIME)
        try:
            # Retry
            doc_ref.update(*args, **kwargs)
            return SUCCESS
        except Exception as identifier:
            # log error
            __log_exception(2, doc_ref, identifier, True)
            return ERROR


def stream(collection_ref, *args, **kwargs):
    """Returns a Firestore stream for a specified collection or query.
    Args:
        collection_ref (object): Firestore reference to a collection
    Returns:
        stream [object]: Firestore stream object.
        -1 [int]: exception (error after retrying a second time)
    """
    try:
        return collection_ref.stream(*args, **kwargs)
    except Exception as identifier:
        # log error
        __log_exception(1, collection_ref, identifier)
        # Wait before continue
        sleep(EXCEPTION_SLEEP_TIME)
        try:
            # Retry
            return collection_ref.stream(*args, **kwargs)
        except Exception as identifier:
            # log error
            __log_exception(1, collection_ref, identifier, True)
            return ERROR



def collection_exists(collection_ref: CollectionReference):
    """A helper method to check whether a collection exists

    Args:
        collection_ref (CollectionReference): The reference object to check if exists

    Returns:
        bool: True if it exists, False otherwise.
    Examples:
        >>> db = new_connection(project=FIRESTORE_PROJECT_ID)
        >>> col_ref = db.collection("MY_COLLECTION")
        >>> print(fc.collection_exists(col_ref))
        False
    """
    docs = get(collection_ref.limit(1))

    if docs == -1:
        logging.error("Couldn't get collection_ref. Please check the logs above for possible errors.")
        return False
    return len(docs) > 0


from google.cloud.firestore_v1.document import DocumentReference


def get_history_doc_refs(
    db: firestore.Client,
    finalurl_or_dealroomid: str
) -> Tuple[DocumentReference]:
    """Returns a DocumentReference based on the final_url field or dealroom_id
    field.

    Args:
        db (firestore.Client): the client that will perform the operations.
        finalurl_or_dealroomid (str): either a domain or a dealroom ID. Query documents that match this parameter.

    Returns:
        Tuple[DocumentReference]: a sequence of document references matching the input parameter.

    Examples:
        >>> db = new_connection(project=FIRESTORE_PROJECT_ID)
        >>> doc_refs = get_history_refs(db, "dealroom.co")
    """

    collection_path = "history"
    collection_ref = db.collection(collection_path)

    if finalurl_or_dealroomid.isnumeric():
        query_params = ["dealroom_id", "==", int(finalurl_or_dealroomid)]
    else:
        query_params = ["final_url", "==", finalurl_or_dealroomid]

    query = collection_ref.where(*query_params)
    docs = stream(query)
    if docs == -1:
        logging.error("Couldn't stream query.")
        return False

    return tuple(doc.reference for doc in docs)


def __log_exception(error_code, ref, identifier, was_retried=False):
    message = "Unknown error"
    if error_code == 1:
        message = f"An error occurred retrieving stream for collection {ref}."
    elif error_code == 2:
        message = f"An error occurred updating document {ref}."
    elif error_code == 3:
        message = f"An error occurred getting document {ref}."
    elif error_code == 4:
        message = f"An error occurred creating document {ref}."
    elif error_code == 5:
        message = f"Error connecting with db with credentials file {ref}."

    if was_retried:
        # TODO save to csv or json
        error_logger(message, error_code)
    else:
        logging.error(f"[Error code {error_code}] {message} Retrying...")
