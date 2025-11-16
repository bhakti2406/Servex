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
def get_all_words(w): #this function is used to seperate the whole word into the list of differnt words
    words = []
    delimiters = ", !.?;"
    for line in w.split('\n'):
        words.extend(re.split(f"[{re.escape(delimiters)}]", line))
    words.remove('')
    l = []
    for a in words:
        if a != '':
            l.append(a)
    return l

# Define the scopes
SCOPES = ['https://www.googleapis.com/auth/drive']
#accessing google drive service
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
    service = build('drive', 'v3', credentials=creds)
    return service


#funtion for downloading the file directly from the google drive to the server side   
def FileDownload(service, file_id, file_name):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=204800)
    done = False

    try:
        while not done:
            done = downloader.next_chunk()

        fh.seek(0)
        with open(file_name, 'wb') as f:
            shutil.copyfileobj(fh, f)

        print("File Downloaded")
        return True
    except:
        print("Something went wrong.")
        return False


#this function will send the slave the list of words to count
def send_for_word_count(slave,l,queue):
    ans = slave.getMap(l)
    queue.put(ans)


#this is the utility function for continuity of the connection of client
def goInfinite(c,name):
    while True:
        op = c.recv(1024).decode()
        if op == "1": #client want the access the word count service
            WordCountFunction(c,name)
        else: #client want to access the matrix multiplication service
            MatrixMultiplicationFunction(c,name)

def WordCountFunction(c,name):
    service = get_gdrive_service()
    allSlaves = []
    q = multiprocessing.Queue() #get the connection status of slaves
    p1 = [] 
    for n in name:
        try:
            p = multiprocessing.Process(target=ConnectSlave,args=(n,q)) #try to connect the respective slave
            p.start()
            p1.append(p)
        except Exception as e:
            continue
    for p in p1:
        p.join()
    while not q.empty():
        allSlaves.append(q.get())
    # receive the id of file of client which is uploaded in google drive
    id = str(c.recv(1024).decode())
    # print(allSlaves)
    FileDownload(service, id, f"{id}.txt") #download the file in this PC
    f = open(f"{id}.txt",encoding='utf8') #open the downloaded file to read it
    w = ""
    for line in f:
        w = w + line
    f.close()
    os.remove(f"{id}.txt")
    words = get_all_words(w) #convert into the list of different words
    n = len(name)
    each_words_count = int(len(words)/n) #dividing the list based on the number of slaves
    st = 0
    i = 0
    queue = multiprocessing.Queue() #this queue stores the answer arriving from all the requested slaves
    processes = []
    while st < len(words):
        try:
            slave = allSlaves[i]
            if each_words_count == 0:
                en = st
            else:
                en = min(st + each_words_count - 1, len(words)-1)
            if en >= len(words):break
            l = []
            j = st
            st = en + 1
            while j <= en:
                l.append(words[j])
                j = j + 1
            p = multiprocessing.Process(target=send_for_word_count, args=(slave,l,queue)) #send the word to this slave
            p.start()
            processes.append(p)
        except Exception as e: #if any error while connecting to this slave
            print(f"Slave {i+1}:error")
        
        i = ((i + 1)%len(allSlaves))

    for p in processes:
        p.join()
    
    d = {}
    #now merge the response from each slave
    while not queue.empty():
        s = queue.get()
        u = s.split(" ")
        for y in u:
            t = y.split(":")
            if len(t) == 1:
                break 
            d[t[0]]=d.get(t[0],0) + int(t[1])
    s = ""
    for k in d.keys():
        print(f"{k}: {d[k]}")
        s = s + k + ":" + str(d[k]) + " "
    c.send(bytes(s,'utf-8'))

def send_for_matrix(slave,i,m1,matrix,queue): #this function is used to send the matrix to the particular slave
    ans = slave.matmul(m1,matrix)
    l = ans.split(" ")
    l1 = [i]
    for h in l:
        l1.append(int(h))
    queue.put(l1)

def ConnectSlave(name,queue): #this function will be used to connect to the respective slaves
    try:
        ns = Pyro4.locateNS('10.0.2.15')
        uri = ns.lookup(name)
        s = Pyro4.Proxy(uri)
        try:
            print(f"{name}:{s.getStatus()}")
            queue.put(s)
        except Exception as e:
            print(f"Error while connecting {name}")
    except Exception as e:
        print(f"Error while connecting {name}")
def MatrixMultiplicationFunction(c,name):
    matrix = b''
    allSlaves = []
    matrix += c.recv(1024)
    q = multiprocessing.Queue() #merging all the connected slaves in this queue
    p1 = [] 
    for n in name:
        try:
            p = multiprocessing.Process(target=ConnectSlave,args=(n,q)) #try to connect to all the slave parallely
            p.start()
            p1.append(p)
        except Exception as e:
            continue
    for p in p1:
        # print("herhe")
        p.join()
    while not q.empty():
        allSlaves.append(q.get())
    matrix = pickle.loads(matrix)
    queue = multiprocessing.Queue()
    c.send(bytes("ack",'utf-8'))
    matrix1 = b''
    matrix1 += c.recv(1024)
    matrix1 = pickle.loads(matrix1) #unpickle the matrix sended by the client
    process = []
    n = 0
    i = 0
    while True:
        if i == len(matrix):
            break
        try:
            slave = allSlaves[n]
            print(f"Connected with Slave {n + 1}: {slave.getStatus()}") #getting the battery status
            p = multiprocessing.Process(target=send_for_matrix, args=(slave,i,matrix[i],matrix1,queue)) #send each row of the first matrix and whole second matrix to the respective slave and store the response in the queue
            p.start()
            process.append(p)
            i = i + 1
        except Exception as e:
            print(f"Error while connecting Slave {n + 1}")
            pass
        n = (n + 1)%len(allSlaves)
    
    for p in process:
        p.join()
    ans = [[0]*len(matrix1[0])]*len(matrix) #store the final answer from each slave in the variable
    while not queue.empty():
        l = queue.get()
        l1=[]
        for i in range(1,len(l)):
            l1.append(l[i])
        ans[l[0]] = l1
    print(ans)
    msg=pickle.dumps(ans) #create a pickle object and send it to the client
    c.send(msg)


if __name__ == "__main__":
    s = socket.socket() #create a socket
    print("Socket created")

    name = ["slave1","slave2","slave3"] #get the name of each slave

    s.bind((socket.gethostbyname(socket.gethostname()),8000)) #bind the server to the port 8000
    print(f"IP: {socket.gethostbyname(socket.gethostname())}")
    s.listen(1)

    print("Waiting for connection")

    while True:
        c,addr = s.accept() #client connected to the server
        print("Connection made with ",addr)
        multiprocessing.Process(target = goInfinite,args=(c,name)).start()

    s.close()
        
