#!/usr/bin/env bash

# Generates a 'setup.py' from 'pyproject.toml' with correct version.
# Version is defined as: git tag and increase patch version +1.
# Creates a folder 'dist/' but does not clean up: it is already git-ignored and
# it's safer not to have 'rm' in script.
# Run in repository folder.

# Usage:
# ./generate_setup.py.sh


ARCHIVE_PATH="./dist/*.tar.gz"


# get latest tag on branch
latest_tag=`git describe --tags`

# increase version + 1 (to PATCH number)
py_commands_to_increase_patch_version="
import sys

# on a non-main branch the tag is like 'v1.0.20-1-g85e38e1'
tag = sys.argv[1].split('-', 1)[0]

# only keep the version numbers
version_numbers = tag.strip('v').split('.')
version_numbers[2] = str(int(version_numbers[2]) + 1)
print('.'.join(version_numbers))
"
new_version=`python -c "$py_commands_to_increase_patch_version" "$latest_tag"`

# add this version to the .toml file
sed '/^version =/s/.*/version = \"'${new_version}'\"/' pyproject.toml > pyproject-temp.toml
mv pyproject-temp.toml pyproject.toml

# build the dist
poetry build

# get 'setup.py' from dist and replace current one
path_to_setup_file_in_archive=`tar -tf $ARCHIVE_PATH | grep -w -- "setup.py"`
tar --strip-components=1 -xvzf $ARCHIVE_PATH "$path_to_setup_file_in_archive"
