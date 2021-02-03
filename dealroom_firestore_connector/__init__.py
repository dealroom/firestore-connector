""" Firestore connector for exception handling with Dealroom data"""
import logging
import os
import traceback
from time import sleep
from typing import Tuple, Union

from dealroom_urlextract import extract
from google.cloud import firestore
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference

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

HISTORY_COLLECTION_PATH = "history"

def get_history_doc_refs(
    db: firestore.Client,
    finalurl_or_dealroomid: str
) -> Union[Tuple[DocumentReference], int]:
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

    collection_ref = db.collection(HISTORY_COLLECTION_PATH)

    if str(finalurl_or_dealroomid).isnumeric():
        query_params = ["dealroom_id", "==", int(finalurl_or_dealroomid)]
    else:
        # Extract the final_url in the required format, so we can query the collection with the exact match.
        try:
            final_url = extract(finalurl_or_dealroomid)
        except:
            logging.error(f"'final_url': {finalurl_or_dealroomid} is not a valid url")
            return ERROR
        query_params = ["final_url", "==", final_url]

    query = collection_ref.where(*query_params)
    docs = stream(query)
    if docs == ERROR:
        logging.error("Couldn't stream query.")
        return ERROR

    return tuple(doc.reference for doc in docs)


def _validate_dealroomid(dealroom_id: Union[str, int]):
    if not str(dealroom_id).isnumeric() and dealroom_id != -1:
        raise ValueError("'dealroom_id' must be an integer")

def _validate_final_url(final_url: str):
    # Extract method has internally validation rules. Check here:
    # https://github.com/dealroom/data-urlextract/blob/main/dealroom_urlextract/__init__.py#L33-L35
    try:
        extract(final_url)
    except:
        raise ValueError("'final_url' must have a url-like format")

def _validate_new_history_doc_payload(payload: dict):
    """Validate the required fields in the payload when creating a new document"""

    if "final_url" not in payload:
        raise KeyError("'final_url' must be present payload")
    
    _validate_final_url(payload["final_url"])
    
    if "dealroom_id" not in payload:
        raise KeyError("'dealroom_id' must be present payload")

    _validate_dealroomid(payload["dealroom_id"])

def _validate_update_history_doc_payload(payload: dict):
    """Validate the required fields in the payload when updating"""

    if "final_url" in payload:
        _validate_final_url(payload["final_url"])
    
    if "dealroom_id" in payload:
        _validate_dealroomid(payload["dealroom_id"])


def set_history_doc_refs(
    db: firestore.Client,
    payload: dict,
    finalurl_or_dealroomid: str = None
) -> None:
    """Updates or creates a document in history collection

    Args:
        db (firestore.Client): the client that will perform the operations.
        payload (dict): The actual data that the newly created will have or the fields to update.
                        'final_url' or 'dealroom_id' is required to find the correct document to set.
        finalurl_or_dealroomid (str): either a domain or a dealroom ID. Query documents that match this parameter.

    Examples:
        >>> db = new_connection(project=FIRESTORE_PROJECT_ID)
        >>> set_history_refs(db, {"final_url": "dealroom.co", "dealroom_id": "1111111")
    """

    history_col = db.collection(HISTORY_COLLECTION_PATH)
    
    _payload = { **payload }

    # If finalurl_or_dealroomid is not provided then a new document will be created.
    history_refs = get_history_doc_refs(db, finalurl_or_dealroomid) if finalurl_or_dealroomid else []

    if history_refs == -1:
        return ERROR
    # CREATE: If there is not available documents in history
    elif len(history_refs) == 0:
        # Add any default values to the payload
        _payload = {
            "dealroom_id": -1,
            **payload
        }
        # Validate that the new document will have the minimum required fields
        try:
            _validate_new_history_doc_payload(_payload)
        except (ValueError, KeyError) as ex:
            logging.error(ex)
            return ERROR
        history_ref = history_col.document()
  
    # UPDATE:
    elif len(history_refs) == 1:
        try:
            _validate_update_history_doc_payload(_payload)
        except ValueError as ex:
            logging.error(ex)
            return ERROR

        history_ref = history_refs[0]
    # If more than one document were found then it's an error.
    else:
         # TODO: Raise a Custom Exception (DuplicateDocumentsException) with the same message when we replace ERROR constant with actual exceptions
        logging.error("Found more than one documents to update for this payload")
        return ERROR
    
    # Ensure that dealroom_id is type of number
    if "dealroom_id" in _payload:
        _payload["dealroom_id"] = int(_payload["dealroom_id"])
    res = set(history_ref, _payload)
        
    if res == -1:
        # TODO: Raise a Custom Exception (FirestoreException) with the same message when we replace ERROR constant with actual exceptions
        logging.error(f"Couldn't `set` document {domain}. Please check logs above.")
        return ERROR

    return SUCCESS
   





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
