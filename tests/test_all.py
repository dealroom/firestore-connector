import random
import string

import dealroom_firestore_connector as fc
import pytest


def _get_random_string(length):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length)).lower()

TEST_PROJECT = "sustained-hold-288413" # Replace with a project ID for testing

def test_collection_exists():
    db = fc.new_connection(project=TEST_PROJECT) 
    col_ref = db.collection("NOT_EXISTING_COLLECTION")
    assert fc.collection_exists(col_ref) == False


def test_set_history_doc_refs_empty_final_url():
    db = fc.new_connection(project=TEST_PROJECT) 
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(KeyError, match=r"'final_url'"):
    res = fc.set_history_doc_refs(db, {
        "dealroom_id": "123123"
    })
    assert res == -1 

def test_set_history_doc_refs_wrong_final_url():
    db = fc.new_connection(project=TEST_PROJECT) 
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(Exception):
    res = fc.set_history_doc_refs(db, {
        "final_url": "asddsadsdsd",
        "dealroom_id": "123123"
    })
    assert res == -1


def test_set_history_doc_refs_empty_dealroom_id():
    db = fc.new_connection(project=TEST_PROJECT) 
    random_field = _get_random_string(10)
    fc.set_history_doc_refs(db, {
        "final_url": "foo2.bar",
        "test_field": random_field
    })
    

def test_set_history_doc_refs_wrong_dealroom_id():
    db = fc.new_connection(project=TEST_PROJECT) 
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(ValueError, match=r"'dealroom_id'"):
    res = fc.set_history_doc_refs(db, {
        "final_url": "foo2.bar",
        "dealroom_id": "foobar"
    })
    assert res == -1

def test_set_history_doc_refs_new():
    db = fc.new_connection(project=TEST_PROJECT) 
    fc.set_history_doc_refs(db, {
        "final_url": f"{_get_random_string(10)}.com"
    })

def test_set_history_doc_refs_update_by_final_url():
    db = fc.new_connection(project=TEST_PROJECT) 
    random_field = _get_random_string(10)
    fc.set_history_doc_refs(db, {
        "final_url": "foo.bar",
        "test_field": random_field
    })
    