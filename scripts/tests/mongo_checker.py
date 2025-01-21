from confluent_kafka import Consumer
import json
import socket
import time
import os
from datetime import datetime

UDP_IP = os.getenv('DOCKER_HOST_IP')

if UDP_IP is None:
    print("Error: DOCKER_HOST_IP environment variable is not set.")
    exit(1)

TIM_UDP_PORT = 47900
TIM_MESSAGE = "001f45201000000000019b3915c0807299b9ea847a9b9dea2001fffe4fd0f23a000078005253373d508f5373bd4400325fffe05b94cdcf53e3d4dcef30e7aa58000002250e1a8880"

BSM_UDP_PORT = 46800
BSM_MESSAGE = "00142500616f47534d21266e861c1ea6e0c780007ffffffff0007080fdfa1fa1007fff80005f11d0"

MAP_UDP_PORT = 44920
MAP_MESSAGE = "0012825f38013020304bda054cdcf8a03d4dc408118602dc05117862c00913a1208b9f965e7cc4800000800005581e530239cbb717fc009ec6c02269c24127767e5979f3240000038000153cfb1808e22edfdf66027d8960b0000802b032000402d83a0009b64100d600c8af09045cfcb2f3e6640000040001289ba95c11ce11c74ea089da88047034b4d8084c584824eecfcb2f3e6880000070001a853257411d8080db2a10bd5f822eeb904695ac2d83c0009b64081160a400040ab0140002065805229c241173f2cbcf9a90000010001891721081a788284b267aa03e03e023018817a2380f60ea095c2504f44103630dc1a89f80e5d4d0f4b602ac336f0027d83cb99ab603133a12093bb3f2cbcf9b2000001c0014a5fc2ca069ba0a0a2f14e03d45401de04015727c0da13e08a48805225282ee3081c01ec1da2b4d78b86f3013ec4b018000807581a0004046c1c8004db21012b00e44584822e7e5979f372000002000115aa6c6808e20b3adc35592ce8e05050b5fddd55acef8c05050b4e605db0209af09049dd9f965e7ce1000000e00085732b938239c2c147521656baf2b541dd40a0a565cbdd814142e2d8171679c28ab44a1e809f60fb39381fad31496070000814b05400040b580900020620049539043e5bf9f3ef8766b000080000027ba7e004739251e5c8239c8014545410f96fe7cfbe1d9ac0002000000a353ad0047392c74c48239c8016553410f96fe7cfbe1d9ac0002000000a3d95630678a0a4fb34d608f1000600004000001590ab2e08f1264eeab04789004919ba7262d9bf1eb410f96fe7cfbe1d9ac0002000000bff2aec419e282930d358833c4fb00"

SPAT_UDP_PORT = 44910
SPAT_MESSAGE = "001374003842BE5E7D1049DDD32F2E7971F4D3BF7097B4080033C76C31660580082080BF7000810D0602E627242C08202FDC0080200608202FDC0040434182498B590C02080C124020080282080BF7001810D0602E627243408202FDC0080200E08683049316B00804341824990090E02080C124020080"

print("UDP target IP:", UDP_IP)

def send_udp_message(port, message):
    print(f"Sending UDP message to port {port}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(bytes.fromhex(message), (UDP_IP, port))

def create_consumer(topic, group_id, bootstrap_servers):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer

def main():
    # Create output directory if it doesn't exist
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create output file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"received_messages_{timestamp}.json")
    
    kafka_bootstrap_servers = f'{UDP_IP}:9092'
    
    # Configure Kafka consumers for each message type
    consumers = {
        'TIM': create_consumer('topic.OdeTimJson', 'ode-checker-tim-group', kafka_bootstrap_servers),
        'BSM': create_consumer('topic.OdeBsmJson', 'ode-checker-bsm-group', kafka_bootstrap_servers),
        'MAP': create_consumer('topic.OdeMapJson', 'ode-checker-map-group', kafka_bootstrap_servers),
        'SPAT': create_consumer('topic.OdeSpatJson', 'ode-checker-spat-group', kafka_bootstrap_servers)
    }

    # Message type to UDP port and message mapping
    message_configs = {
        'TIM': {'port': TIM_UDP_PORT, 'message': TIM_MESSAGE},
        'BSM': {'port': BSM_UDP_PORT, 'message': BSM_MESSAGE},
        'MAP': {'port': MAP_UDP_PORT, 'message': MAP_MESSAGE},
        'SPAT': {'port': SPAT_UDP_PORT, 'message': SPAT_MESSAGE}
    }

    # Dictionary to store received messages
    received_data = {msg_type: None for msg_type in consumers.keys()}

    # Send one message of each type
    print("\nSending UDP messages...")
    for msg_type, config in message_configs.items():
        send_udp_message(config['port'], config['message'])
        time.sleep(0.5)  # Small delay between messages

    print("\nWaiting for Kafka messages...")
    try:
        # Set a timeout of 10 seconds for receiving messages
        end_time = time.time() + 10
        messages_received = set()

        while time.time() < end_time and len(messages_received) < len(consumers):
            for msg_type, consumer in consumers.items():
                if msg_type in messages_received:
                    continue

                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    # Parse and store the message value
                    value = json.loads(msg.value().decode('utf-8'))
                    received_data[msg_type] = value
                    print(f"\nReceived {msg_type} message:")
                    print(json.dumps(value, indent=2))
                    messages_received.add(msg_type)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON for {msg_type}: {e}")
                except Exception as e:
                    print(f"Error processing message for {msg_type}: {e}")
        
        # Save received messages to file
        with open(output_file, 'w') as f:
            json.dump(received_data, f, indent=2)
        print(f"\nReceived messages saved to: {output_file}")
        
        # Report results
        print("\nResults:")
        for msg_type in consumers.keys():
            status = "✓ Received" if msg_type in messages_received else "✗ Not received"
            print(f"{msg_type}: {status}")
                    
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Close all consumers
        for consumer in consumers.values():
            consumer.close()

if __name__ == "__main__":
    main()