import dealroom_firestore_connector as fc

TEST_PROJECT = "TEST_PROJECT" # Replace with a project ID for testing

def test_collection_exists():
    db = fc.new_connection(project=TEST_PROJECT) 
    col_ref = db.collection("NOT_EXISTED_COLLECTION")
    assert fc.collection_exists(col_ref) == False
