import sys

import dealroom_firestore_connector as fc

# Connect to staging Firestore
db = fc.new_connection(
    project="sustained-hold-288413",
    credentials_path="cred.json",
)
if db == -1:
    # Error. Do something else
    sys.exit()

# Go to desired collection
collection_ref = db.collection(u"firestore_connector_test")

# Create a document
doc_ref = collection_ref.document(u"test_file")
if fc.set(doc_ref, {u"website": u"abc.com", u"index": u"123123"}) == -1:
    # Error. Do something else (i.e. continue to next doc)
    sys.exit()
else:
    pass


# Get a document
doc_ref = collection_ref.document(u"test_file")
doc = fc.get(doc_ref)
if doc != -1:
    print(doc.id)
    print(doc.to_dict())

# Update a document
if fc.update(doc_ref, {u"website": u"123.com"}) != -1:
    pass  # Continue if no error in update call
else:
    sys.exit()

# Stream a collection
stream = fc.stream(collection_ref)
if stream != -1:
    for doc in stream:
        print(doc.id)
        print(doc.to_dict())
else:
    sys.exit()
