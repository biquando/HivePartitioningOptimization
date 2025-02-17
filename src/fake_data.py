import csv
import random
from faker import Faker

fake = Faker()


# Generate Users
def generate_users(n=1000):
    users = []
    for _ in range(n):
        users.append(
            [
                fake.unique.random_int(min=1, max=n * 1000),
                fake.name(),
                fake.email(),
                fake.date_time_between(start_date="-2y", end_date="now").isoformat(),
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
def generate_orders(n=2000, user_ids=[]):
    orders = []
    for _ in range(n):
        orders.append(
            [
                fake.unique.random_int(min=1, max=n * 1000),
                random.choice(user_ids),
                fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
                round(random.uniform(20.0, 1000.0), 2),
            ]
        )
    return orders


# Generate Order Items
def generate_order_items(orders, products):
    order_items = []
    for order in orders:
        num_items = random.randint(1, 5)
        selected_products = random.sample(products, num_items)
        for product in selected_products:
            quantity = random.randint(1, 3)
            order_items.append(
                [
                    order[0],  # order_id
                    product[0],  # product_id
                    quantity,
                    product[3],  # price
                ]
            )
    return order_items


# Generate Reviews
def generate_reviews(n=3000, user_ids=[], product_ids=[]):
    reviews = []
    for _ in range(n):
        reviews.append(
            [
                fake.unique.random_int(min=1, max=n * 1000),
                random.choice(user_ids),
                random.choice(product_ids),
                random.randint(1, 5),
                fake.sentence(),
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
