# firestore-connector
A wrapper class for accessing Google Cloud Firestore based on our business logic

## Install
`pip install -e git+https://github.com/dealroom/firestore-connector.git#egg=dealroom-firestore-connector`

## Usage
```python
import dealroom_firestore_connector as fc

# Equivalent to firestore.Client()
db = fc.new_connection()

doc_ref = db.collection('...').document('...').get()

# payload to update
payload = {}

# Update a doc
fc.set(doc_ref, payload)
```
