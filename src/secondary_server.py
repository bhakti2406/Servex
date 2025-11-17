from __future__ import print_function
import socket
import Pyro4
import multiprocessing
import re
import os
import pickle
import os.path
import io
import shutil
from mimetypes import MimeTypes
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

def get_all_words(w):  # split words
    words = []
    delimiters = ", !.?;"
    for line in w.split('\n'):
        words.extend(re.split(f"[{re.escape(delimiters)}]", line))
    return [a for a in words if a.strip() != '']


SCOPES = ['https://www.googleapis.com/auth/drive']


def get_gdrive_service():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('./client_secrets.json', SCOPES)
            creds = flow.run_local_server(port=8080)

        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)


def FileDownload(service, file_id, file_name):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request, chunksize=204800)

        done = False
        while not done:
            done = downloader.next_chunk()

        fh.seek(0)
        with open(file_name, 'wb') as f:
            shutil.copyfileobj(fh, f)

        print("File downloaded.")
        return True

    except Exception as e:
        print("Error downloading:", e)
        return False


def send_for_word_count(slave, l, queue):
    queue.put(slave.getMap(l))


def goInfinite(c, name):
    while True:
        op = c.recv(1024).decode()
        if not op:
            break
        if op == "1":
            WordCountFunction(c, name)
        else:
            MatrixMultiplicationFunction(c, name)


def WordCountFunction(c, name):
    service = get_gdrive_service()
    allSlaves = []
    q = multiprocessing.Queue()

    processes = []
    for n in name:
        p = multiprocessing.Process(target=ConnectSlave, args=(n, q))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    while not q.empty():
        allSlaves.append(q.get())

    file_id = c.recv(1024).decode()
    FileDownload(service, file_id, f"{file_id}.txt")

    with open(f"{file_id}.txt", encoding='utf8') as f:
        w = f.read()

    os.remove(f"{file_id}.txt")

    words = get_all_words(w)
    each = max(1, len(words) // len(allSlaves))

    result_queue = multiprocessing.Queue()
    tasks = []

    i = 0
    while i < len(words):
        for slave in allSlaves:
            segment = words[i:i + each]
            if not segment:
                break
            
            p = multiprocessing.Process(
                target=send_for_word_count, args=(slave, segment, result_queue)
            )
            p.start()
            tasks.append(p)

            i += each

    for p in tasks:
        p.join()

    final_map = {}
    while not result_queue.empty():
        for pair in result_queue.get().split():
            if ":" in pair:
                word, count = pair.split(":")
                final_map[word] = final_map.get(word, 0) + int(count)

    response = " ".join(f"{k}:{v}" for k, v in final_map.items())
    c.send(response.encode())


def send_for_matrix(slave, i, row, matrix2, q):
    values = list(map(int, slave.matmul(row, matrix2).split()))
    q.put([i] + values)


def ConnectSlave(name, queue):
    try:
        ns = Pyro4.locateNS("127.0.0.1")
        uri = ns.lookup(name)
        slave = Pyro4.Proxy(uri)
        slave.getStatus()
        queue.put(slave)
    except Exception:
        print(f"Slave {name} unavailable")


def MatrixMultiplicationFunction(c, name):
    allSlaves = []
    q = multiprocessing.Queue()

    processes = []
    for n in name:
        p = multiprocessing.Process(target=ConnectSlave, args=(n, q))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    while not q.empty():
        allSlaves.append(q.get())

    matrix1 = pickle.loads(c.recv(1024))
    c.send(b"ack")

    matrix2 = pickle.loads(c.recv(65535))

    result_q = multiprocessing.Queue()
    tasks = []

    for i in range(len(matrix1)):
        p = multiprocessing.Process(
            target=send_for_matrix,
            args=(allSlaves[i % len(allSlaves)], i, matrix1[i], matrix2, result_q)
        )
        p.start()
        tasks.append(p)

    for p in tasks:
        p.join()

    result = [[0] * len(matrix2[0]) for _ in range(len(matrix1))]

    while not result_q.empty():
        row = result_q.get()
        result[row[0]] = row[1:]

    c.send(pickle.dumps(result))


# ---------------- MAIN SERVER START ----------------

if __name__ == "__main__":
    SERVER_IP = "10.42.29.144"  # same as primary
    SERVER_PORT = 8010          # different port from primary

    s = socket.socket()
    print("Secondary server socket created.")

    s.bind((SERVER_IP, SERVER_PORT))
    print(f"Secondary server running at {SERVER_IP}:{SERVER_PORT}")

    s.listen(1)
    print("Waiting for connection from primary server...")

    while True:
        conn, addr = s.accept()
        print("Connected:", addr)
        multiprocessing.Process(target=goInfinite, args=(conn, ["slave1", "slave2", "slave3"])).start()

    s.close()
