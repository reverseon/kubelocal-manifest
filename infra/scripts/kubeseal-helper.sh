#!/bin/bash

# Global Variables

G_SCRIPT_DIR=$(dirname $0)
G_INFRA_FILES_DIR=$G_SCRIPT_DIR/../files

while getopts "e:d:" flag; do
  case $flag in
    e)
      kubeseal --cert $G_INFRA_FILES_DIR/crt.pub.pem -o yaml < "$OPTARG"
      ;;
    d)
      kubeseal --recovery-unseal --recovery-private-key <(gpg -dq $G_INFRA_FILES_DIR/key.pem.gpg) -o yaml < "$OPTARG"
      ;;
  esac
done
