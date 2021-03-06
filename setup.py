# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['dealroom_firestore_connector']

package_data = \
{'': ['*']}

install_requires = \
['dealroom-urlextract @ git+https://github.com/dealroom/data-urlextract@main',
 'google-cloud-firestore>=2.0.2,<3.0.0']

setup_kwargs = {
    'name': 'dealroom-firestore-connector',
    'version': '1.0.21',
    'description': 'A wrapper class for accessing Google Cloud Firestore based on our business logic',
    'long_description': '# firestore-connector\nA wrapper class for accessing Google Cloud Firestore based on our business logic\n\n## Install\n**pip**:  \n`pip install -e git+https://github.com/dealroom/data-firestore-connector.git#egg=dealroom-firestore-connector`  \n\n**Poetry**:  \n`poetry add "git+https://github.com/dealroom/data-firestore-connector#main"`\n\n:exclamation: This package requires installing _[dealroom-urlextract](https://github.com/dealroom/data-urlextract)_. If you install firestore-connector using Poetry then, when generating requirements.txt you should make sure that _dealroom-urlextract_ is installed correctly like this: `dealroom-urlextract @ git+https://github.com/dealroom/data-urlextract.git@main`. You\'ll have to change this manually.\n\n```suggestion\n-dealroom-urlextract==0.1.0\n+dealroom-urlextract @ git+https://github.com/dealroom/data-urlextract@main#egg=dealroom_urlextract\n```\n\nThis is an issue from Poetry.\n\n## Usage\n\n### Simple\n```python\nimport dealroom_firestore_connector as fc\n\n# Equivalent to firestore.Client()\ndb = fc.new_connection(project="...")\n\ncollection_ref = db.collection("...")\ndoc_ref = collection_ref.document("...")\n\n# Get a doc\ndoc = fc.get(doc_ref)\n\n# payload to update\npayload = {}\n\n# Update a doc\nfc.set(doc_ref, payload)\n```\n\n### Batched\n```python\nimport dealroom_firestore_connector as fc\n\n# Equivalent to firestore.Client()\ndb = fc.new_connection(project="...")\n\ncollection_ref = db.collection("...")\n\n# -------\n# Option 1: Sequentially\n\nbatch = fc.Batcher(db)\n# Writes/Deletes\nbatch.set(collection_ref.document("doc1"), {"foo1": "bar1"})\nbatch.set(collection_ref.document("doc2"), {"foo2": "bar2"})\nbatch.update(collection_ref.document("doc3"), {"foo3": "bar3"})\n\nstatus = batch.commit()\n\nif status < 0:\n    print("Failed to commit changes")\nelse:\n    print("Successful commited changes")\n\n# -----\n# Option 2: Using context manager pattern\n\nwith fc.Batcher(db) as batch:\n    batch.set(collection_ref.document("doc1"), {"foo1": "bar1"})\n    batch.set(collection_ref.document("doc2"), {"foo2": "bar2"})\n    batch.update(collection_ref.document("doc3"), {"foo3": "bar3"})\n```\n\nSee [examples.py](examples.py) for more examples\n\n---\n\n## Contributing\nEvery time a change is made we should bump up the version of the module at [setup.py](setup.py).\n',
    'author': 'Dealroom.co',
    'author_email': 'data@dealroom.co',
    'maintainer': None,
    'maintainer_email': None,
    'url': 'https://github.com/dealroom/data-firestore-connector',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.8,<4.0',
}


setup(**setup_kwargs)
