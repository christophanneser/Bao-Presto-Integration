"""This module connects to the host 'ntp' and requests its time for synchronization."""
import socket
import struct
import datetime
import sys
import time

HOST = 'ntp'
PORT = 3333


def recvall(sock, size, buffer=bytes()):
    while len(buffer) < size:
        part = sock.recv(size - len(buffer))
        buffer += part
    return buffer


def receive_time(sock):
    start = time.time()
    sock.sendto(b"1", (HOST, PORT))
    buffer = recvall(sock, 8)
    rec_time = struct.unpack('>q', buffer[:8])[0]
    end = time.time()
    rtt = (end - start) * 1000
    print("RTT={0:.2f}ms".format(rtt))
    return rec_time, rtt


def request_ntp_time():
    for _ in range(10):
        try:
            print("request time from ntp")
            s = socket.socket()  # Create a socket object
            s.connect((HOST, PORT))

            rec_time, rtt = receive_time(s)
            estimated_next_rec_time = rec_time + rtt / 2
            estimated_time = datetime.datetime.fromtimestamp(estimated_next_rec_time / 1000 + rtt / 2000,
                                                             tz=datetime.timezone.utc)
            t = datetime.datetime.now()
            return estimated_time.strftime("@%Y-%m-%d %H:%M:%S")
        except:
            print("could not connect, try again in 1 sec")
            time.sleep(1)

if __name__ == '__main__':
    request_ntp_time()
