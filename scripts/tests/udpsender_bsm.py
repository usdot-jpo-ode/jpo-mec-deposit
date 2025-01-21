import socket
import time
import os

# Currently set to oim-dev environment's ODE
UDP_IP = os.getenv('DOCKER_HOST_IP')
UDP_PORT = 46800
MESSAGE = "00142500616f47534d21266e861c1ea6e0c780007ffffffff0007080fdfa1fa1007fff80005f11d0"

print("UDP target IP:", UDP_IP)
print("UDP target port:", UDP_PORT)
#print("message:", MESSAGE)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP

frequency = 1

print(f"Sending BSM at {frequency} Hz")
while True:
  sock.sendto(bytes.fromhex(MESSAGE), (UDP_IP, UDP_PORT))
  time.sleep(1 / frequency)
