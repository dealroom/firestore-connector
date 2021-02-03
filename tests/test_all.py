import random
import string
from random import randint

import dealroom_firestore_connector as fc
import pytest
from dealroom_firestore_connector.status_codes import ERROR, SUCCESS


def _get_random_string(length):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length)).lower()

TEST_PROJECT = "sustained-hold-288413" # Replace with a project ID for testing

def test_collection_exists():
    db = fc.new_connection(project=TEST_PROJECT) 
    col_ref = db.collection("NOT_EXISTING_COLLECTION")
    assert fc.collection_exists(col_ref) == False


def test_set_history_doc_refs_empty_final_url():
    """Creating a new document, without a final_url should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT) 
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(KeyError, match=r"'final_url'"):
    res = fc.set_history_doc_refs(db, {
        "dealroom_id": "123123"
    })
    assert res == ERROR

def test_set_history_doc_refs_wrong_final_url():
    """Creating a new document, with invalid final_url should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT) 
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(Exception):
    res = fc.set_history_doc_refs(db, {
        "final_url": "asddsadsdsd",
        "dealroom_id": "123123"
    })
    assert res == ERROR


def test_set_history_doc_refs_new():
    """Creating a new document, with valid final_url & w/o dealroom id should be ok"""
    db = fc.new_connection(project=TEST_PROJECT) 
    res = fc.set_history_doc_refs(db, {
        "final_url": f"{_get_random_string(10)}.com"
    })

    assert res == SUCCESS

def test_set_history_doc_refs_new():
    """Creating a new document, with valid final_url & valid dealroom id should be ok"""
    db = fc.new_connection(project=TEST_PROJECT) 
    res = fc.set_history_doc_refs(db, {
        "final_url": f"{_get_random_string(10)}.com",
        "dealroom_id": randint(1e5, 1e8)
    })

    assert res == SUCCESS

def test_set_history_doc_refs_empty_dealroom_id():
    """Updating a new document, using a valid final_url, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT) 
    random_field = _get_random_string(10)
    res = fc.set_history_doc_refs(db, {
        "test_field": random_field
    }, "foo2.bar")
    assert res == SUCCESS
    
def test_set_history_doc_refs_empty_final_url():
    """Updating a new document, using a valid dealroom_id, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT) 
    random_field = _get_random_string(10)
    EXISTING_DOC_DR_ID = "10000000000023"
    
    res = fc.set_history_doc_refs(db, {
        "test_field": random_field
    }, EXISTING_DOC_DR_ID)

    assert res == SUCCESS
    

def test_set_history_doc_refs_wrong_dealroom_id():
    """Creating a new document, with invalid dealroomid should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT) 
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(ValueError, match=r"'dealroom_id'"):
    res = fc.set_history_doc_refs(db, {
        "final_url": "foo2.bar",
        "dealroom_id": "foobar"
    })
    assert res == -1

