# 🔖 Consistent Hashing

This project implements a **Consistent Hashing-based Distributed Key-Value Storage System** to simulate an efficient and scalable flat naming service. The system comprises a **Bootstrap Name Server (BNS)** and multiple **Name Servers (NS)**, collaboratively handling dynamic key-value operations such as insertion, lookup, and deletion.

## 🔷 Features
- **Dynamic Scalability**: Seamlessly supports node entry and exit with minimal data redistribution.
- **Fault Tolerance**: Ensures continuous operation by maintaining predecessor-successor links.
- **Efficient Key Distribution**: Uses consistent hashing to evenly distribute keys across the hash ring.
- **Socket-Based Communication**: Facilitates interaction between nodes in the distributed system.

## 🔷 Key Operations
1. **Insert**: Adds a key-value pair to the appropriate server based on the hash ring.
2. **Lookup**: Efficiently retrieves the value associated with a key.
3. **Delete**: Removes a key-value pair from the system.
4. **Node Entry**: Dynamically integrates new servers while redistributing relevant keys.
5. **Node Exit**: Gracefully removes servers, transferring keys to their successors.

## 🔷 Experimental Setup
- **Programming Language**: Java (21.0.2)
- **IDE**: Eclipse 4.12
- **System Configuration**: macOS Monterey, Apple M2, 8 GB RAM
- **Testing**: Conducted on a fixed key range `[0-1023]` with dynamic scenarios involving node addition, removal, and key-value operations.

## 🔷 Future Enhancements
- Implementing secure hashing techniques like **SHA-256** for improved security.
- Extending support for multimedia data (e.g., images, videos) to handle diverse workloads.
- Adding virtual servers to enhance scalability and fault tolerance.

## 🔷 Referneces
- [Consistent Hashing](https://youtu.be/UF9Iqmg94tk?si=xIXjNntvbkFqL1zY)
