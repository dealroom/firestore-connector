""" Firestore connector for exception handling with Dealroom data"""
import logging
import os
import traceback
from time import sleep

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
    docs = collection_ref.limit(1).get()
    return len(docs) > 0


def __log_exception(error_code, ref, identifier, was_retried=False):
    message = "Unknown error"
    if error_code == 1:
        message = "An error occurred retrieving stream for collection %s." % (ref)
    elif error_code == 2:
        message = "An error occurred updating document %s." % (ref)
    elif error_code == 3:
        message = "An error occurred getting document %s." % (ref)
    elif error_code == 4:
        message = "An error occurred creating document %s." % (ref)
    elif error_code == 5:
        message = "Error connecting with db with credentials file %s." % (ref)

    if was_retried:
        # TODO save to csv or json
        error_logger(error_code, message)
    else:
        logging.error("[Error code %d] %s Retrying..." % (error_code, message))
