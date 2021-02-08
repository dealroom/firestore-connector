# firestore-connector
A wrapper class for accessing Google Cloud Firestore based on our business logic

## Install
**pip**:  
`pip install -e git+https://github.com/dealroom/data-firestore-connector.git#egg=dealroom-firestore-connector`  

**Poetry**:  
`poetry add "git+https://github.com/dealroom/data-firestore-connector#main"`

:exclamation: This package requires installing _[dealroom-urlextract](https://github.com/dealroom/data-urlextract)_. If you install firestore-connector using Poetry then, when generating requirements.txt you should make sure that _dealroom-urlextract_ is installed correctly like this: `dealroom-urlextract @ git+https://github.com/dealroom/data-urlextract.git@main`. You'll have to change this manually.

```suggestion
-dealroom-urlextract==0.1.0
+dealroom-urlextract @ git+https://github.com/dealroom/data-urlextract@main#egg=dealroom_urlextract
```

This is an issue from Poetry.

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
