.PHONY: run clean small medium large users products orders order_items reviews

# Default values
DATA_SIZE=3
ALGORITHM=1
TABLES=

# Add optional tables argument if specified
ifdef TABLES
  TABLES_ARG=--tables=$(TABLES)
else
  TABLES_ARG=
endif

start:
	./hive.sh

connect:
	docker exec -it hive4 beeline -u 'jdbc:hive2://localhost:10000/'

# Run the script with command-line arguments
# make run DATA_SIZE=10 ALGORITHM=1 TABLES=users,orders
run:
	python src/testbench.py --data_size=$(DATA_SIZE) --algorithm=$(ALGORITHM) $(TABLES_ARG)

clean:
	rm -r algorithm_reports/*