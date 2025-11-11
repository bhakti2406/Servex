# Distributed Master-Slave Client-Server Architecture

This project demonstrates a distributed system using a master-slave (client-server) architecture in Python. It supports distributed word count and matrix multiplication operations, leveraging Pyro4 for remote object communication and Google Drive API for file handling.

## Features

- **Distributed Word Count:** Upload a text file, split the workload among slave nodes, and aggregate word frequencies.
- **Distributed Matrix Multiplication:** Enter matrices via GUI, distribute computation across slaves, and collect results.
- **Google Drive Integration:** Upload/download files for processing using Google Drive API.
- **Fault Tolerance:** Supports multiple slaves and backup server.
- **GUI Client:** User-friendly interface built with Tkinter.

## Architecture

- **Client (`src/client.py`):** GUI for users to submit tasks and view results.
- **Primary Server (`src/primary_server.py`):** Coordinates tasks, splits workloads, and aggregates results.
- **Secondary Server (`src/secondary_server.py`):** Backup server for redundancy.
- **Slaves (`src/slave1.py`, `src/slave2.py`, `src/slave3.py`):** Worker nodes that process assigned tasks.
- **Google Drive API:** Used for file upload/download.
- **Pyro4 Name Server:** Registers slaves and enables remote method invocation.

## Directory Structure

```
Master_Slave-Client_Server_Architecture/
├── client.py
├── primary_server.py
├── secondary_server.py
├── slave1.py
├── slave2.py
├── slave3.py
├── client_secrets.json
├── token.pickle
├── src/
│   └── temp/
│       └── a.txt
└── run_instructions .txt
```

## Setup Instructions

### 1. Install Python

Python 3.7 or later is required.

```sh
sudo apt update
sudo apt install python3 python3-pip
```

### 2. Install Required Packages

```sh
pip install tkinter socket threading google-auth \
google-auth-oauthlib google-auth-httplib2 \
google-api-python-client PyPDF2 Pyro4 psutil
```

### 3. Google Drive API Setup

- Enable **Google Drive API** in [Google Cloud Console](https://console.developers.google.com/).
- Create OAuth 2.0 credentials and download `src/client_secrets.json`.
- Place `src/client_secrets.json` in the project root.
- On first run, authenticate in browser; `token.pickle` will be generated.

### 4. Start Pyro4 Name Server

```sh
python3 -m Pyro4.naming -n <your_ip>
```

### 5. Start Slave Nodes

Open a new terminal for each slave:

```sh
python3 slave1.py
python3 slave2.py
python3 slave3.py
```

### 6. Start Primary Server

```sh
python3 primary_server.py
```

### 7. (Optional) Start Secondary Server

```sh
python3 secondary_server.py
```

### 8. Start Client Application

```sh
python3 client.py
```

## Usage

### Matrix Multiplication

1. Launch the client GUI.
2. Click "Matrix Multiplication".
3. Enter matrix dimensions and values.
4. Submit to start distributed computation.
5. View results in the GUI.

### Word Count

1. Launch the client GUI.
2. Click "Word Count".
3. Upload a `.txt` file.
4. View word frequency results in the GUI.

## Dependencies

- Python 3.7+
- tkinter
- socket
- threading
- google-auth
- google-auth-oauthlib
- google-auth-httplib2
- google-api-python-client
- PyPDF2
- Pyro4
- psutil

## Notes

- Ensure all scripts run on the same network or configure IPs accordingly.
- The client authenticates with Google Drive on first run.
- Temporary files are stored in `src/temp`.

## License

This project is for educational purposes.

---

**For detailed step-by-step instructions, see `run_instructions .txt`.**
