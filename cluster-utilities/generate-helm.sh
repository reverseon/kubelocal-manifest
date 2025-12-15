#!/bin/bash

SCRIPT_DIR=$(cd $(dirname $0); pwd)
FOLDER=$1

if [ ! -d "$SCRIPT_DIR/$FOLDER" ]; then
    echo "Folder $SCRIPT_DIR/$FOLDER does not exist"
    exit 1
fi

helmfile -f $SCRIPT_DIR/$FOLDER/helmfile.yaml template --include-crds > $SCRIPT_DIR/$FOLDER/helm-$FOLDER.yaml