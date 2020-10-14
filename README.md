# firestore-connector
A wrapper class for accessing Google Cloud Firestore based on our business logic

## Install
`pip install -e git+https://github.com/dealroom/firestore-connector.git#egg=dealroom-firestore-connector`

## Usage
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

See [examples.py](examples.py) for more examples

---

## Contributing
Every time a change is made we should bump up the version of the module at [setup.py](setup.py).