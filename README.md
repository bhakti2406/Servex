# Servex

This project demonstrates a distributed system using a master-slave (client-server) framework in Python. It supports distributed word count and matrix multiplication operations, leveraging Pyro4 for remote object communication.

## Features

- **Distributed Word Count:** Upload a text file, split the workload among slave nodes, and aggregate word frequencies.
- **Distributed Matrix Multiplication:** Enter matrices via GUI, distribute computation across slaves, and collect results.
- **Fault Tolerance:** Supports multiple slaves and backup server.
- **GUI Client:** User-friendly interface built with Tkinter.

## Architecture

- **Client (`src/client.py`):** GUI for users to submit tasks and view results.
- **Primary Server (`src/primary_server.py`):** Coordinates tasks, splits workloads, and aggregates results.
- **Secondary Server (`src/secondary_server.py`):** Backup server for redundancy.
- **Slaves (`src/slave1.py`, `src/slave2.py`, `src/slave3.py`):** Worker nodes that process assigned tasks.
- **Pyro4 Name Server:** Registers slaves and enables remote method invocation.

## Directory Structure

```
Servex/
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
pip install tkinter socket threading
```

### 3. Start Pyro4 Name Server

```sh
python3 -m Pyro4.naming -n <your_ip>
```

### 4. Start Slave Nodes

Open a new terminal for each slave:

```sh
python3 slave1.py
python3 slave2.py
python3 slave3.py
```

### 5. Start Primary Server

```sh
python3 primary_server.py
```

### 6. (Optional) Start Secondary Server

```sh
python3 secondary_server.py
```

### 7. Start Client Application

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
- PyPDF2
- Pyro4
- psutil

## Notes

- Ensure all scripts run on the same network or configure IPs accordingly.
- Temporary files are stored in `src/temp`.

## License

This project is for educational purposes.

---

**For detailed step-by-step instructions, see `run_instructions .txt`.**
