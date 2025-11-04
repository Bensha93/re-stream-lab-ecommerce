"""
Pub/Sub Event Publisher - Test Script
---------------------------------------------------------------------------------
Publishes test events (orders, inventory, user_activity) to Pub/Sub topic
"""

from google.cloud import pubsub_v1
import json
from datetime import datetime
import uuid
import time

# -------------------------------------------------------------------------------------------------------------
# CONFIGURATION - Updated with your project details / Pub/Sub topic / project id: re-stream-lab-ecommerce
# -------------------------------------------------------------------------------------------------------------
PROJECT_ID = "re-stream-lab-ecommerce"
TOPIC = "backend-events-topic"

# Initialize publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)

print(f"Publishing to: {topic_path}\n")


# ---------------------------------------------------------------------------
# TEST : ORDER EVENT / print details for verification
# ---------------------------------------------------------------------------
def publish_order_event():
    """Publish a test order event"""
    order_event = {
        "event_type": "order",
        "order_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "order_date": datetime.utcnow().isoformat() + "Z",
        "status": "pending",
        "items": [
            {
                "product_id": str(uuid.uuid4()),
                "product_name": "Wireless Keyboard",
                "quantity": 1,
                "price": 79.99
            },
            {
                "product_id": str(uuid.uuid4()),
                "product_name": "USB Mouse",
                "quantity": 2,
                "price": 29.99
            }
        ],
        "shipping_address": {
            "street": "123 Main Street",
            "city": "San Francisco",
            "country": "USA"
        },
        "total_amount": 139.97
    }
    
    data = json.dumps(order_event).encode('utf-8')
    future = publisher.publish(topic_path, data)
    message_id = future.result()
    print(f" ORDER Published - Message ID: {message_id}")
    print(f"   Order ID: {order_event['order_id']}")
    print(f"   Total: ${order_event['total_amount']}\n")
    return message_id


# ---------------------------------------------------------------------------
# TEST : INVENTORY EVENT / print details for verification
# ---------------------------------------------------------------------------
def publish_inventory_event():
    """Publish a test inventory event"""
    inventory_event = {
        "event_type": "inventory",
        "inventory_id": str(uuid.uuid4()),
        "product_id": str(uuid.uuid4()),
        "warehouse_id": "warehouse-sf-01",
        "quantity_change": -5,  # Negative = sold/used
        "reason": "sale",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    data = json.dumps(inventory_event).encode('utf-8')
    future = publisher.publish(topic_path, data)
    message_id = future.result()
    print(f" INVENTORY Published - Message ID: {message_id}")
    print(f"   Inventory ID: {inventory_event['inventory_id']}")
    print(f"   Change: {inventory_event['quantity_change']} ({inventory_event['reason']})\n")
    return message_id


# ---------------------------------------------------------------------------
# TEST : USER ACTIVITY EVENT / print details for verification
# ---------------------------------------------------------------------------
def publish_user_activity_event():
    """Publish a test user activity event"""
    activity_event = {
        "event_type": "user_activity",
        "user_id": str(uuid.uuid4()),
        "activity_type": "add_to_cart",
        "ip_address": "192.168.1.100",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "metadata": {
            "session_id": str(uuid.uuid4()),
            "platform": "web"
        }
    }
    
    data = json.dumps(activity_event).encode('utf-8')
    future = publisher.publish(topic_path, data)
    message_id = future.result()
    print(f" USER ACTIVITY Published - Message ID: {message_id}")
    print(f"   User ID: {activity_event['user_id']}")
    print(f"   Activity: {activity_event['activity_type']}\n")
    return message_id


# ----------------------------------------------------------------------------
# MAIN - Publish all test events / if any errors occur / print troubleshooting
# ----------------------------------------------------------------------------
if __name__ == '__main__':
    print("=" * 70)
    print(" PUBLISHING TEST EVENTS TO PUB/SUB")
    print("=" * 70)
    print()
    
    try:
        # Publish one of each event type
        publish_order_event()
        time.sleep(1)  # Small delay between messages
        
        publish_inventory_event()
        time.sleep(1)
        
        publish_user_activity_event()
        
        print("=" * 70)
        print(" ALL EVENTS PUBLISHED SUCCESSFULLY!")
        print("=" * 70)
        print()
        print(" Next Steps:")
        print("1. Check Dataflow job for processing:")
        print("   https://console.cloud.google.com/dataflow/jobs")
        print()
        print("2. Verify data in BigQuery (wait 30-60 seconds):")
        print("   bq query --use_legacy_sql=false \\")
        print("     'SELECT * FROM `re-stream-lab-ecommerce.backend_events.orders` LIMIT 5'")
        print()
        print("   bq query --use_legacy_sql=false \\")
        print("     'SELECT * FROM `re-stream-lab-ecommerce.backend_events.inventory` LIMIT 5'")
        print()
        print("   bq query --use_legacy_sql=false \\")
        print("     'SELECT * FROM `re-stream-lab-ecommerce.backend_events.user_activity` LIMIT 5'")
        
    except Exception as e:
        print(f" ERROR: {e}")
        print("\nTroubleshooting:")
        print("1. Authentication: gcloud auth application-default login")
        print("2. Pub/Sub topic exists: gcloud pubsub topics describe backend-events-topic")
        print("3. Check permissions on the topic")