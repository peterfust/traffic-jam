import socket
import time

class Netcat:

    def __init__(self, ip, port):

        self.buff = ""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((ip, port))
        self.socket.listen(10)
        self.conn, addr = self.socket.accept()

    def write(self, data):
        # append newline between messages
        data = data + "\n"
        # encode before sending through socket
        message = data.encode()
        self.conn.send(message)

    def close(self):
        self.socket.close()

def send(nc):
    # RUN ONLY 3 iterations per each window of 1 second to make sure that wordcount works as expected
    i = 0
    while True:
        try:
            print("Sending iteration "+ str(i))
            nc.write('A,B,12')
            nc.write('B,C,4')
            nc.write("C,D,6")
            i+=1
            if i == 3:
                time.sleep(1)
                i = 0
        except Exception as e:
            print(str(e))
            break

while True:
    nc = Netcat("127.0.0.1", 9999)
    try:
        send(nc)
    except Exception as e:
        print(str(e))
    finally:
        nc.close()

