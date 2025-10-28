# 🧩 MiniKafka — A Lightweight Kafka Clone in Java

MiniKafka is a **lightweight, educational implementation of a Kafka-like message broker**, built entirely from scratch in Java.

It re-creates the **core fundamentals** of Apache Kafka — persistent logs, topic and partition management, segment rolling, and TCP-based message passing — to help you *learn how Kafka actually works under the hood.*

---

## 🚀 Features

- **Append-only log storage**
  - Durable, persistent log segments on disk (`0.log`, `10000.log`, ...)
- **CRC validation**
  - Ensures data integrity for every message
- **Automatic segment rolling**
  - New `.log` files created once the active one is full
- **Topic management**
  - Dynamically creates topics via a simple `PartitionManager`
- **TCP protocol**
  - Produce and fetch messages using a simple text protocol via `nc` (netcat)
- **Readable and educational**
  - Minimal dependencies, pure Java 21

---

## 🧠 Architecture Overview


| Layer | Class | Description |
|--------|--------|-------------|
| **Record** | `Record.java` | Represents a key-value message |
| **LogSegment** | `LogSegment.java` | Manages one `.log` file on disk |
| **PartitionLog** | `PartitionLog.java` | Manages multiple segments per topic |
| **PartitionManager** | `PartitionManager.java` | Manages all topics (1 partition per topic) |
| **BrokerHandler / BrokerServer** | `BrokerHandler.java`, `BrokerServer.java` | Handle TCP connections and client requests |

---

## ⚙️ Getting Started

### 1️⃣ Clone the repository
```bash
git clone https://github.com/jai8gupta/mini-kafka.git
cd mini-kafka
```

mvn clean compile

mvn exec:java -Dexec.mainClass=com.minilog.broker.BrokerServer

💬 Usage Example

In another terminal, connect with nc (netcat):

nc localhost 9092


Then you can start producing and fetching messages:

Produce messages
PRODUCE topic0 name Jai
PRODUCE topic0 city Delhi

Fetch messages
FETCH topic0 0 10

Quit session
QUIT


Expected output:

OK offset=0
OK offset=1
name=Jai
city=Delhi
END
BYE
