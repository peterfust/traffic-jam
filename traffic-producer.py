import math
import socket
import time
from common_values import demo_edges
import matplotlib.pyplot as plt
import numpy as np

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

traffic_factor = 2

def sinus_traffic_value(t, cl):
    normalized_t = t / cl
    sin_value = math.sin( 2 * math.pi * normalized_t)
    scaled_value = round(5 * (sin_value))
    if scaled_value >= 0:
        return scaled_value * traffic_factor
    return 0

def plot_sinus_and_current_value(t_values, current_t, current_value):
    sinus_values = [sinus_traffic_value(t, curve_length) for t in t_values]
    plt.plot(t_values, sinus_values, label='Sinuskurve')
    plt.scatter([current_t], [current_value], color='red')  # Markieren des aktuellen Werts
    plt.legend()
    plt.show()

curve_length = 100


def send(producer):
    varianz = 0
    plus = True
    while True:
        try:
            for i, location in enumerate(demo_edges):
                current_t = varianz + i * curve_length / len(demo_edges)
                value = sinus_traffic_value(current_t, curve_length)
                print(f"Der Stau zwischen {location[0]} und {location[1]} beträgt {value} min")
                producer.write(f"{location[0]},{location[1]},{value}")
                t_values = np.arange(0, curve_length, 0.1)
                plot_sinus_and_current_value(t_values, current_t, value)
                # 10 Sekunden warten für nächste Prognose
                time.sleep(10)

            # varianz verändern, damit es spannender wird, immer zwischen 0 → 24 → 0
            if varianz == 24:
                plus = False
            elif varianz == 0:
                plus = True
            if plus:
                varianz += 1
            else:
                varianz -= 1
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

