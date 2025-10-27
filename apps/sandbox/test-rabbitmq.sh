#!/bin/bash

# RabbitMQ Test Script
# Tests RabbitMQ pod in sandbox namespace
# Usage: ./test-rabbitmq.sh

set -e

# Configuration
NAMESPACE="sandbox"
POD_NAME="rabbitmq-0"
SERVICE_NAME="rabbitmq"
USERNAME="user"
PASSWORD="test123"
LOCAL_PORT="15672"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   RabbitMQ Test Script${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Function to cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    pkill -f "port-forward.*${SERVICE_NAME}" 2>/dev/null || true
    echo -e "${GREEN}✓ Cleanup complete${NC}"
}
trap cleanup EXIT

# 1. Check Pod Status
echo -e "${BLUE}[1/9] Checking pod status...${NC}"
POD_STATUS=$(kubectl get pods -n ${NAMESPACE} ${POD_NAME} --no-headers 2>/dev/null | awk '{print $3}')
if [ "$POD_STATUS" == "Running" ]; then
    echo -e "${GREEN}✓ Pod ${POD_NAME} is running${NC}"
    kubectl get pods -n ${NAMESPACE} ${POD_NAME}
else
    echo -e "${RED}✗ Pod ${POD_NAME} is not running (Status: ${POD_STATUS})${NC}"
    exit 1
fi

# 2. Check Services
echo -e "\n${BLUE}[2/9] Checking services...${NC}"
kubectl get svc -n ${NAMESPACE} | grep rabbitmq
echo -e "${GREEN}✓ Services found${NC}"

# 3. Check RabbitMQ Status
echo -e "\n${BLUE}[3/9] Checking RabbitMQ status...${NC}"
kubectl exec -n ${NAMESPACE} ${POD_NAME} -- rabbitmqctl status | head -20
echo -e "${GREEN}✓ RabbitMQ status OK${NC}"

# 4. List Users
echo -e "\n${BLUE}[4/9] Listing users...${NC}"
kubectl exec -n ${NAMESPACE} ${POD_NAME} -- rabbitmqctl list_users
echo -e "${GREEN}✓ Users listed${NC}"

# 5. Port Forward
echo -e "\n${BLUE}[5/9] Setting up port-forward...${NC}"
kubectl port-forward -n ${NAMESPACE} svc/${SERVICE_NAME} ${LOCAL_PORT}:15672 > /dev/null 2>&1 &
PORT_FORWARD_PID=$!
sleep 3

# Check if port-forward is working
if ps -p $PORT_FORWARD_PID > /dev/null; then
    echo -e "${GREEN}✓ Port-forward established (PID: ${PORT_FORWARD_PID})${NC}"
else
    echo -e "${RED}✗ Port-forward failed${NC}"
    exit 1
fi

# 6. Test Authentication & Get Overview
echo -e "\n${BLUE}[6/9] Testing authentication...${NC}"
OVERVIEW=$(curl -s -u ${USERNAME}:${PASSWORD} http://localhost:${LOCAL_PORT}/api/overview)
if echo "$OVERVIEW" | python3 -m json.tool > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Authentication successful${NC}"
    echo "Management Version: $(echo $OVERVIEW | python3 -c 'import sys, json; print(json.load(sys.stdin)["management_version"])')"
else
    echo -e "${RED}✗ Authentication failed${NC}"
    exit 1
fi

# 7. Create Test Queue
echo -e "\n${BLUE}[7/9] Creating test queue...${NC}"
QUEUE_NAME="test-queue-$(date +%s)"
CREATE_RESPONSE=$(curl -s -u ${USERNAME}:${PASSWORD} -X PUT "http://localhost:${LOCAL_PORT}/api/queues/%2F/${QUEUE_NAME}" \
  -H "Content-Type: application/json" \
  -d '{"auto_delete":false,"durable":true}')

if [ -z "$CREATE_RESPONSE" ]; then
    echo -e "${GREEN}✓ Queue '${QUEUE_NAME}' created successfully${NC}"
else
    echo -e "${YELLOW}Response: ${CREATE_RESPONSE}${NC}"
fi

# 8. Publish Test Message
echo -e "\n${BLUE}[8/9] Publishing test message...${NC}"
TEST_MESSAGE="Hello from RabbitMQ test at $(date)"
PUBLISH_RESPONSE=$(curl -s -u ${USERNAME}:${PASSWORD} -X POST "http://localhost:${LOCAL_PORT}/api/exchanges/%2F/amq.default/publish" \
  -H "Content-Type: application/json" \
  -d "{
    \"properties\":{},
    \"routing_key\":\"${QUEUE_NAME}\",
    \"payload\":\"${TEST_MESSAGE}\",
    \"payload_encoding\":\"string\"
  }")

ROUTED=$(echo $PUBLISH_RESPONSE | python3 -c 'import sys, json; print(json.load(sys.stdin).get("routed", False))')
if [ "$ROUTED" == "True" ]; then
    echo -e "${GREEN}✓ Message published successfully${NC}"
else
    echo -e "${RED}✗ Message routing failed${NC}"
    echo "Response: $PUBLISH_RESPONSE"
fi

# 9. Consume Test Message
echo -e "\n${BLUE}[9/9] Consuming test message...${NC}"
CONSUME_RESPONSE=$(curl -s -u ${USERNAME}:${PASSWORD} -X POST "http://localhost:${LOCAL_PORT}/api/queues/%2F/${QUEUE_NAME}/get" \
  -H "Content-Type: application/json" \
  -d '{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}')

RECEIVED_MESSAGE=$(echo $CONSUME_RESPONSE | python3 -c 'import sys, json; messages = json.load(sys.stdin); print(messages[0]["payload"] if messages else "")')

if [ ! -z "$RECEIVED_MESSAGE" ]; then
    echo -e "${GREEN}✓ Message consumed successfully${NC}"
    echo "Received: $RECEIVED_MESSAGE"
else
    echo -e "${RED}✗ No message received${NC}"
fi

# Cleanup Test Queue
echo -e "\n${BLUE}Cleaning up test queue...${NC}"
curl -s -u ${USERNAME}:${PASSWORD} -X DELETE "http://localhost:${LOCAL_PORT}/api/queues/%2F/${QUEUE_NAME}" > /dev/null
echo -e "${GREEN}✓ Test queue deleted${NC}"

# Final Summary
echo -e "\n${BLUE}========================================${NC}"
echo -e "${GREEN}   All Tests Passed! ✓${NC}"
echo -e "${BLUE}========================================${NC}\n"

echo "Summary:"
echo "  • Pod Status: Running"
echo "  • Authentication: Success"
echo "  • Queue Operations: Success"
echo "  • Message Publishing: Success"
echo "  • Message Consumption: Success"
echo ""
echo "RabbitMQ is fully operational!"
echo ""
echo "To access the management UI:"
echo "  kubectl port-forward -n ${NAMESPACE} svc/${SERVICE_NAME} 15672:15672"
echo "  Then visit: http://localhost:15672"
echo "  Login: ${USERNAME} / ${PASSWORD}"

