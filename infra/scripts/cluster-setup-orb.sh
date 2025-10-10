#!/bin/zsh

# Global Variables

G_SCRIPT_DIR=$(dirname $0)
G_CLUSTER_UTILS_DIR=$G_SCRIPT_DIR/../../cluster-utilities

# Pre-utilities

# Utilities
kubectl apply -k $G_CLUSTER_UTILS_DIR/sealed-secrets
kubectl apply -k $G_CLUSTER_UTILS_DIR/ingress-nginx
kubectl apply -k $G_CLUSTER_UTILS_DIR/cert-manager
kubectl apply -k $G_CLUSTER_UTILS_DIR/argocd

# Post-utilities

ARGOCD_PLAIN_PASSWORD=$(gpg -dq $G_SCRIPT_DIR/../files/argocd-admin-pass.txt.gpg | tr -d '\n\r')
ARGOCD_BCRYPT_PASSWORD=$(argocd account bcrypt --password "$ARGOCD_PLAIN_PASSWORD")

kubectl -n argocd patch secret argocd-secret \
  -p '{"stringData": {
    "admin.password": "'"$ARGOCD_BCRYPT_PASSWORD"'",
    "admin.passwordMtime": "'"$(date -u +%FT%TZ)"'"
  }}'

kubectl -n sealed-secrets create secret tls general-tls --cert $G_SCRIPT_DIR/../files/crt.pub.pem --key $G_SCRIPT_DIR/../files/key.pem
kubectl -n sealed-secrets label secret general-tls sealedsecrets.bitnami.com/sealed-secrets-key=active
kubectl -n sealed-secrets rollout restart deployment.apps/sealed-secrets

# Pre-bootstrap

# Bootstrap

# Post-bootstrap

printf "%s\n" "NGINX Controller IP: $(kubectl get svc -n ingress-nginx ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"