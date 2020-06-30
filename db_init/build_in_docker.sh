#!/bin/bash

rm -rf build
mkdir build

cp build.sh requirements.txt __main__.py build
docker run -it -v $PWD/build:/app --user 1000 --rm -w /app mcr.microsoft.com/azure-cli:2.0.80 bash -c 'export HOME=/tmp; export PATH=$HOME/.local/bin:$PATH; python3 -m pip install -U --user pip; /app/build.sh --no-zip'
python3 -m zipapp --compress --output=build/db_init.pyz build/build