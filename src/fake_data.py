# fake_data.py
import csv
from datetime import datetime
import random
from faker import Faker
from datetime import timedelta

# For each table, there is guaranteed to be one column with
# at most this cardinality
CARDINALITY = 1000


fake = Faker()


# Generate Users
def generate_users(
    n=1000,
    id_cardinality=500,
    name_cardinality=40,
    email_cardinality=11,
    date_cardinality=18,
):
    # Set default cardinalities if not specified
    id_cardinality = id_cardinality or n
    name_cardinality = name_cardinality or n
    email_cardinality = email_cardinality or n
    date_cardinality = date_cardinality or n

    users = []

    # Create pools of values for each column to ensure cardinality constraints
    id_pool = [i for i in range(1, id_cardinality + 1)]
    name_pool = [f"User{i}" for i in range(1, name_cardinality + 1)]
    email_pool = [f"user{i}@example.com" for i in range(1, email_cardinality + 1)]

    # Generate dates pool
    base_date = datetime.now()
    date_pool = [base_date - timedelta(days=i) for i in range(date_cardinality)]

    for _ in range(n):
        users.append(
            [
                random.choice(id_pool),
                random.choice(name_pool),
                random.choice(email_pool),
                random.choice(date_pool),
            ]
        )

    return users


# Generate Products
# Generate Products
def generate_products(n=500):
    products = []
    categories = [
        "Electronics",
        "Clothing",
        "Home & Kitchen",
        "Books",
        "Sports",
        "Toys & Games",
        "Beauty",
        "Automotive",
        "Garden & Outdoor",
        "Pet Supplies",
        "Health",
        "Grocery",
        "Tools",
        "Music",
        "Movies",
        "Baby",
        "Office Products",
        "Jewelry",
        "Art & Crafts",
        "Industrial",
    ]
    for _ in range(n):
        products.append(
            [
                fake.unique.random_int(min=1, max=n * 1000),
                fake.word().capitalize(),
                random.choice(categories),
                round(random.uniform(5.0, 500.0), 2),
                random.randint(0, 100),
            ]
        )
    return products


# Generate Orders
def generate_orders(
    n=2000,
    user_ids=[],  # Keep this parameter for referential integrity
    id_cardinality=17,
    user_id_cardinality=85,  # This will be constrained by available user_ids
    date_cardinality=41,
    amount_cardinality=99,
):
    # Set default cardinalities
    id_cardinality = id_cardinality or n
    user_id_cardinality = min(user_id_cardinality or len(user_ids), len(user_ids))
    date_cardinality = date_cardinality or 365  # Default to 1 year of dates
    amount_cardinality = amount_cardinality or n

    orders = []

    # Create pools of values
    id_pool = [i for i in range(1, id_cardinality + 1)]
    user_id_pool = (
        random.sample(user_ids, user_id_cardinality)
        if user_ids
        else [i for i in range(1, user_id_cardinality + 1)]
    )

    # Generate date pool
    base_date = datetime.now()
    date_pool = [
        (base_date - timedelta(days=i)).isoformat() for i in range(date_cardinality)
    ]

    # Generate amount pool with even distribution
    amount_pool = [
        round(20.0 + (i * (980.0 / amount_cardinality)), 2)  # 20.0 to 1000.0 range
        for i in range(amount_cardinality)
    ]

    for _ in range(n):
        orders.append(
            [
                random.choice(id_pool),
                random.choice(user_id_pool),
                random.choice(date_pool),
                random.choice(amount_pool),
            ]
        )

    return orders


# Generate Order Items
def generate_order_items(
    orders,
    products,
    items_per_order_cardinality=17,  # How many different numbers of items per order
    product_per_order_cardinality=55,  # How many different products can be in an order
    quantity_cardinality=11,  # How many different quantity values
    price_cardinality=81,  # How many different price points
):
    # Set default cardinalities
    items_per_order_cardinality = items_per_order_cardinality or 5  # Default 1-5 items
    product_per_order_cardinality = product_per_order_cardinality or len(products)
    quantity_cardinality = quantity_cardinality or 3  # Default 1-3 quantities
    price_cardinality = price_cardinality or len(set(p[3] for p in products))

    order_items = []

    # Create pools of values
    items_count_pool = list(range(1, items_per_order_cardinality + 1))
    quantity_pool = list(range(1, quantity_cardinality + 1))

    # Create price pool based on available product prices
    unique_prices = sorted(set(p[3] for p in products))
    if len(unique_prices) > price_cardinality:
        # Take evenly spaced samples if we need to reduce cardinality
        step = len(unique_prices) // price_cardinality
        price_pool = unique_prices[::step][:price_cardinality]
    else:
        price_pool = unique_prices

    # Process each order
    for order in orders:
        # Select number of items for this order
        num_items = random.choice(items_count_pool)

        # Select available products for this order based on cardinality constraint
        available_products = random.sample(
            products, min(product_per_order_cardinality, len(products))
        )

        # Select actual products for this order
        selected_products = random.sample(
            available_products, min(num_items, len(available_products))
        )

        for product in selected_products:
            order_items.append(
                [
                    order[0],  # order_id (maintain referential integrity)
                    product[0],  # product_id (maintain referential integrity)
                    random.choice(quantity_pool),
                    random.choice(price_pool),
                ]
            )

    return order_items


# Generate Reviews
def generate_reviews(
    n=3000,
    user_ids=[],  # Keep for referential integrity
    product_ids=[],  # Keep for referential integrity
    id_cardinality=71,
    user_id_cardinality=91,
    product_id_cardinality=61,
    rating_cardinality=5,
    text_cardinality=20,
):
    # Set default cardinalities
    id_cardinality = id_cardinality or n
    user_id_cardinality = min(user_id_cardinality or len(user_ids), len(user_ids))
    product_id_cardinality = min(
        product_id_cardinality or len(product_ids), len(product_ids)
    )
    rating_cardinality = rating_cardinality or 5  # Default 1-5 ratings
    text_cardinality = text_cardinality or n  # Default unique reviews

    reviews = []

    # Create pools of values
    id_pool = [i for i in range(1, id_cardinality + 1)]
    user_id_pool = (
        random.sample(user_ids, user_id_cardinality)
        if user_ids
        else [i for i in range(1, user_id_cardinality + 1)]
    )
    product_id_pool = (
        random.sample(product_ids, product_id_cardinality)
        if product_ids
        else [i for i in range(1, product_id_cardinality + 1)]
    )
    rating_pool = list(
        range(1, min(rating_cardinality + 1, 6))
    )  # Never exceed 5-star rating
    text_pool = [f"Review text {i}" for i in range(1, text_cardinality + 1)]

    for _ in range(n):
        reviews.append(
            [
                random.choice(id_pool),
                random.choice(user_id_pool),
                random.choice(product_id_pool),
                random.choice(rating_pool),
                random.choice(text_pool),
            ]
        )

    return reviews


def generate_data(size_MiB=10):
    # Generate Data
    print("Generating users...")
    users = generate_users(2000 * size_MiB)
    print("Generating products...")
    products = generate_products(1000 * size_MiB)
    print("Generating orders...")
    orders = generate_orders(4000 * size_MiB, [user[0] for user in users])
    print("Generating order_items...")
    order_items = generate_order_items(orders, products)
    print("Generating reviews...")
    reviews = generate_reviews(
        6000 * size_MiB,
        [user[0] for user in users],
        [product[0] for product in products],
    )

    # Save Data to CSV
    def save_to_csv(filename, data, headers):
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(data)

    save_to_csv("data/users.csv", users, ["user_id", "name", "email", "created_at"])
    save_to_csv(
        "data/products.csv",
        products,
        ["product_id", "name", "category", "price", "stock"],
    )
    save_to_csv(
        "data/orders.csv", orders, ["order_id", "user_id", "order_date", "total_amount"]
    )
    save_to_csv(
        "data/order_items.csv",
        order_items,
        ["order_id", "product_id", "quantity", "price"],
    )
    save_to_csv(
        "data/reviews.csv",
        reviews,
        ["review_id", "user_id", "product_id", "rating", "comment"],
    )

    print("Data generation complete! CSV files are ready.")


if __name__ == "__main__":
    generate_data()
