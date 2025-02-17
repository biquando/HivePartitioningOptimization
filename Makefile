.PHONY: run clean small medium large users products orders order_items reviews

# Default values
DATA_SIZE=2
ALGORITHM=1
TABLES=

# Add optional tables argument if specified
ifdef TABLES
  TABLES_ARG=--tables=$(TABLES)
else
  TABLES_ARG=
endif

# Run the script with command-line arguments
run:
	python src/test.py --data_size=$(DATA_SIZE) --algorithm=$(ALGORITHM) $(TABLES_ARG)

# Example shortcuts
small:
	make run DATA_SIZE=10 TABLES=$(TABLES)

medium:
	make run DATA_SIZE=50 TABLES=$(TABLES)

large:
	make run DATA_SIZE=200 TABLES=$(TABLES)

# Convenience targets for common table combinations
users:
	make run TABLES=users

products:
	make run TABLES=products

orders:
	make run TABLES=orders

order_items:
	make run TABLES=order_items

reviews:
	make run TABLES=reviews

# Example of predefined table combinations
orders-all:
	make run TABLES=orders,order_items,products

user-related:
	make run TABLES=users,orders,reviews

clean:
	rm -r algorithm_reports/*