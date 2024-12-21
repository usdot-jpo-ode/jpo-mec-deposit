import socket
import time
import os

# Currently set to oim-dev environment's ODE
UDP_IP = os.getenv('DOCKER_HOST_IP')
UDP_PORT = 47900
MESSAGE = "001f45201000000000019b3915c0807299b9ea847a9b9dea2001fffe4fd0f23a000078005253373d508f5373bd4400325fffe05b94cdcf53e3d4dcef30e7aa58000002250e1a8880"

print("UDP target IP:", UDP_IP)
print("UDP target port:", UDP_PORT)
#print("message:", MESSAGE)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP

frequency = 10

print(f"Sending BSM at {frequency} Hz")
while True:
  sock.sendto(bytes.fromhex(MESSAGE), (UDP_IP, UDP_PORT))
  time.sleep(1 / frequency)
