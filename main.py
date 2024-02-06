import operator
import socket
import threading
import time
import pickle
from os.path import exists

class PyInMemStore:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()
        self.transaction_buffer = []
        self.transaction_mode = False
    def set_value(self,key,value):
        with self.lock:
            if self.transaction_mode:
                self.transaction_buffer.append(("SET",key,value))
            else:
                self.data[key] = {"value": value, "ttl": None}

    def get_value(self, key):
        with self.lock:
            data = self.data.get(key)
            if (data == None):
                return None
            else:
                return data["value"]

    def delete_key(self, key):
        with self.lock:
            if self.transaction_mode:
                self.transaction_buffer.append(("DELETE", key))
                return "OK"
            else:
                if key in self.data:
                    del self.data[key]
                else:
                    return "Key not found"

    def expire_key(self, key, seconds):
        with self.lock:
            if self.transaction_mode:
                self.transaction_buffer.append(("EXPIRE",key,seconds))
                return "OK"
            else:
               if key in self.data:
                    self.data[key]["ttl"] = time.time() + seconds
                    return "OK"
               else:
                   return "Key not found"
    def ttl_key(self, key):
        with self.lock:
            data = self.data.get(key)
            if data:
                if not data["ttl"]:
                    return -1
                remaining_time = max(data["ttl"] - time.time(), 0)
                return remaining_time
            else:
                return -2

    def begin_transaction(self):
        self.transaction_mode = True

    def commit_transaction(self):
        with self.lock:
            for action in self.transaction_buffer:
                command, *args = action
                if command == "SET":
                    key, value = args
                    self.data[key] = {"value": value, "ttl": None}
                elif command == "DELETE":
                    key, = args
                    if key in self.data:
                        del self.data[key]
                elif command == "EXPIRE":
                    key, seconds = args
                    if key in self.data:
                        self.data[key]["ttl"] = time.time() + seconds

    def rollback_transaction(self):
        self.transaction_buffer = []
        self.transaction_mode = False

    def list(self):
        with self.lock:
            data = self.data
            if data:
                allData="\n"
                for key,value in data.items():
                    allData += ('------  ' + key)
                    allData += (str(value) + '\n')
                return allData
            else:
                return "No Data Exist"

    def sort(self):
        try:
            with self.lock:
                data = dict(sorted(self.data.items(),key=lambda item: item[1]["value"]))
                if data:
                    allData="\n"
                    for key,value in data.items():
                        allData += ('-----  ' + key)
                        allData += (str(value) + '\n')
                    return allData
                else:
                    return "No Data Exist"
        except Exception as e:
            print(str(e))
    def CheckExpire(self):
        data = self.data
        if data:
            for key, value in list(data.items()):
                    if key in self.data:
                        if self.data[key]["ttl"]:
                            remaining_time = max(data[key]["ttl"] - time.time(), 0)
                            if remaining_time == 0:
                                print(f"{key}  Expire")
                                self.data.pop(key)
        return True

    def Save(self):
        with open("MemStore","wb") as f:
            pickle.dump(self.data,f,pickle.HIGHEST_PROTOCOL)


    def Load(self):
        if exists("MemStore"):
            with open("MemStore","rb") as f:
                self.data = pickle.load(f)
                print("Loaded Successfully")

def handle_client(client_socket, store):
    try:
        while True:
            request = client_socket.recv(1024).decode('utf-8')
            if not request:
                break
            response = process_request(request, store)
            client_socket.send(response.encode('utf-8'))
        client_socket.close()
    except Exception as e:
        print(str(e))

def process_request(request, store):
    try:
        parts = request.split()
        command =str(parts[0]).upper()

        if command == "SET":
            if len(parts)==3:
                key, value = parts[1], parts[2]
                store.set_value(key, value)
                return "OK"
            else:
                return "INPUT ERROR"

        elif command == "GET":
            if len(parts)==2:
                key = parts[1]
                value = store.get_value(key)
                return value or "None"
            else:
                return "INPUT ERROR"

        elif command == "DELETE":
            if len(parts)==2:
                key = parts[1]
                return  store.delete_key(key)
            else:
                return "INPUT ERROR"

        elif command == "EXPIRE":
            if len(parts)==3:
                key, seconds = parts[1], int(parts[2])
                return store.expire_key(key, seconds)
            else:
                return "INPUT ERROR"

        elif command == "TTL":
            if len(parts)==2:
                key = parts[1]
                ttl = store.ttl_key(key)
                return str(ttl)
            else:
                return "INPUT ERROR"

        elif command == "BEGIN":
            store.begin_transaction()
            return "Transaction started"

        elif command == "COMMIT":
            store.commit_transaction()
            return "Transaction committed"

        elif command == "ROLLBACK":
            store.rollback_transaction()
            return "Transaction rolled back"

        elif command == "SORT":
            allData=store.sort()
            return allData

        elif command == "LIST":
            allData = store.list()
            return allData

        else:
            return "Invalid command"
    except Exception as e:
        return str(e)


def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 8080))
    server_socket.listen(5)

    print("Server is listening on port 8080...")

    store = PyInMemStore()
    store.Load()

    while True:
        store.CheckExpire()
        client_socket, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")

        client_handler = threading.Thread(target=handle_client, args=(client_socket, store))
        client_handler.start()
        store.Save()


if __name__ == '__main__':
    start_server()
