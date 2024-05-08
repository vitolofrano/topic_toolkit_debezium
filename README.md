# Kafka Topic Management for Debezium

This Python script facilitates the management of Kafka topics specifically designed for integration with Debezium, a tool for capturing data changes from databases. It enables users to create and delete Kafka topics dynamically, based on the tables in a PostgreSQL database that do not have corresponding audit tables.

## Features

- **Create Topics**: Automatically generate Kafka topics for each qualifying table in a PostgreSQL database.
- **Delete Topics**: Remove Kafka topics based on specific conditions.
- **Custom Configurations**: Users can specify the number of partitions, compression type, and cleanup policies for each topic.
- **Interactive Interface**: The script uses command-line prompts to gather database connection settings and topic configurations.

## Prerequisites

Ensure you have Python 3.x installed along with the following Python packages:
- `psycopg2`
- `confluent_kafka`

You can install these packages using pip.

## Usage

To run the script, execute it via the command line.
```bash
py topicToolkit.py 
```
```bash
py topicToolkit.py <host> <port> <dbname> <user> <password>
```
The script prompts for database and Kafka broker details. Operations include:
1. **Create Kafka Topics**: Targets PostgreSQL tables that do not end with 'aud'.
2. **Delete Kafka Topics**: Can be performed based on different criteria, such as all topics or those corresponding to specific tables.
3. **Exit**: Stops the script.

## Configuration

During execution, the script prompts for:
- PostgreSQL host, port, database name, user, and password.
- Kafka broker address.
- Configuration for topics like partition count, compression type, and cleanup policy.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

- [Lofrano Vito](your-contact-information)

## Contributions

Contributions are welcome! Feel free to submit pull requests or open issues for any bugs or improvements.
