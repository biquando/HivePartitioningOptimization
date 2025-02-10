# Hive Partitioning Optimization

## Dependencies

- Docker
- Python3

## Setup

### Starting the Hive container

To start the Hive server in Docker: `./hive.sh`

To connect to Hive via the Beeline cli: `docker exec -it hive4 beeline -u 'jdbc:hive2://localhost:10000/'`

To see the logs in a web browser: http://localhost:10002

### Setting up Python and running the example

```sh
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt

python example.py
```
