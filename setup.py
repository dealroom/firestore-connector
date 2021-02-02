import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dealroom-firestore-connector",  # Replace with your own username
    version="0.0.5",
    author="Dealroom.co",
    author_email="data@dealroom.co",
    description="A wrapper class for accessing Google Cloud Firestore.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dealroom/data-firestore-connector",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "google-cloud-firestore",
        "git+https://github.com/dealroom/data-urlextract@set-history-url#egg=dealroom_urlextract"
    ],
    python_requires=">=3.6",
)
