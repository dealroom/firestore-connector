# firestore-connector
A wrapper class for accessing Google Cloud Firestore based on our business logic

## Install
`pip install -e git+https://github.com/dealroom/firestore-connector.git#egg=dealroom-firestore-connector`

## Usage

### Simple
```python
import dealroom_firestore_connector as fc

# Equivalent to firestore.Client()
db = fc.new_connection(project="...")

collection_ref = db.collection("...")
doc_ref = collection_ref.document("...")

# Get a doc
doc = fc.get(doc_ref)

# payload to update
payload = {}

# Update a doc
fc.set(doc_ref, payload)
```

### Batched
```python
import dealroom_firestore_connector as fc

# Equivalent to firestore.Client()
db = fc.new_connection(project="...")

collection_ref = db.collection("...")

# -------
# Option 1: Sequentially

batch = fc.Batcher(db)
# Writes/Deletes
batch.set(collection_ref.document("doc1"), {"foo1": "bar1"})
batch.set(collection_ref.document("doc2"), {"foo2": "bar2"})
batch.update(collection_ref.document("doc3"), {"foo3": "bar3"})

status = batch.commit()

if status < 0:
    print("Failed to commit changes")
else:
    print("Successful commited changes")

# -----
# Option 2: Using context manager pattern

with fc.Batcher(db) as batch:
    batch.set(collection_ref.document("doc1"), {"foo1": "bar1"})
    batch.set(collection_ref.document("doc2"), {"foo2": "bar2"})
    batch.update(collection_ref.document("doc3"), {"foo3": "bar3"})
```

See [examples.py](examples.py) for more examples

---

## Contributing
Every time a change is made we should bump up the version of the module at [setup.py](setup.py).
