queries = [

# Basic queries

#1
f'SELECT * FROM users',

#2
f'SELECT * FROM products WHERE stock > 0',

#3
f'SELECT * FROM orders WHERE order_date >= DATE_SUB(CURRENT_DATE, 30)',

#4
f'SELECT * FROM reviews WHERE rating = 5',

#5
f'SELECT COUNT(*) FROM users',

# Filtering and Aggregations

#6
f'SELECT category, COUNT(*) FROM products GROUP BY category',

#7
f'SELECT product_id, SUM(quantity * price) AS total_sales FROM order_items GROUP BY product_id',

#8
f'SELECT * FROM products ORDER BY price DESC LIMIT 1',

#9
f'SELECT user_id, COUNT(*) AS order_count FROM orders GROUP BY user_id ORDER BY order_count DESC LIMIT 1',

#10
f'SELECT product_id, COUNT(*) AS review_count FROM reviews GROUP BY product_id ORDER BY review_count DESC LIMIT 5',

#11
f'''SELECT user_id, AVG(total_amount) AS avg_order_value 
FROM orders 
GROUP BY user_id''',

# Join queries

#12
f'''SELECT o.order_id, COUNT(oi.product_id) AS num_items
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id''',

#13
f'SELECT p.name, p.category FROM products p',

#14
f'''SELECT o.order_id, u.name, u.email, o.total_amount
FROM orders o
JOIN users u ON o.user_id = u.user_id''',

#15
f'''SELECT p.product_id, p.name
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
WHERE oi.product_id IS NULL''',

#16
f'''SELECT u.user_id, u.name
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
WHERE o.user_id IS NULL''',

# Sorting and Limits

#17
f'SELECT * FROM products ORDER BY price DESC LIMIT 10',

#18
f'SELECT * FROM orders ORDER BY order_date DESC LIMIT 5',

#19
f'''SELECT o.user_id, SUM(o.total_amount) AS total_spent
FROM orders o
GROUP BY o.user_id
ORDER BY total_spent DESC
LIMIT 5''',

#20
f'''SELECT product_id, AVG(rating) AS avg_rating
FROM reviews
GROUP BY product_id
ORDER BY avg_rating DESC
LIMIT 3''',

#21
f'''SELECT product_id, COUNT(*) AS review_count
FROM reviews
GROUP BY product_id
ORDER BY review_count ASC
LIMIT 5''',

# Advanced Aggregations

#22
f'SELECT AVG(total_amount) FROM orders',

#23
f'''SELECT MONTH(order_date) AS month, SUM(total_amount) AS total_sales
FROM orders
GROUP BY MONTH(order_date)
ORDER BY total_sales DESC
LIMIT 1''',

#24
f'''SELECT p.category, COUNT(*) AS total_sold
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sold DESC
LIMIT 1''',

#25
f'SELECT product_id, AVG(rating) AS avg_rating FROM reviews GROUP BY product_id',

#26
f'SELECT user_id, SUM(total_amount) AS total_revenue FROM orders GROUP BY user_id',

# Window functions

#27
f'''SELECT user_id, COUNT(order_id) AS order_count, RANK() OVER (ORDER BY COUNT(order_id) DESC) AS rank
FROM orders
GROUP BY user_id''',

# f'''SELECT review_id, user_id, product_id, rating, comment, order_date
# FROM (
#     SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY order_date DESC) AS rn
#     FROM reviews
# ) t
# WHERE rn = 1''',

#28
f'SELECT order_date, SUM(total_amount) OVER (ORDER BY order_date) AS cumulative_sales FROM orders',

#29
f'''SELECT user_id, MONTH(order_date) AS month, SUM(total_amount) AS total_spent,
       RANK() OVER (PARTITION BY MONTH(order_date) ORDER BY SUM(total_amount) DESC) AS rank
FROM orders
GROUP BY user_id, MONTH(order_date)''',

#30
f'SELECT user_id, MIN(order_date) AS first_order_date FROM orders GROUP BY user_id',

# Advanced queries and joins

#31
f'''SELECT oi.product_id, SUM(oi.quantity * oi.price) AS total_revenue
FROM order_items oi
GROUP BY oi.product_id''',

#32
f'SELECT user_id, COUNT(order_id) AS order_count FROM orders GROUP BY user_id',

#33
f'''SELECT DISTINCT u.user_id, u.name
FROM users u
JOIN orders o ON u.user_id = o.user_id''',

#34
f'SELECT product_id, COUNT(review_id) AS review_count FROM reviews GROUP BY product_id',

#35
f'SELECT product_id, SUM(quantity) AS total_sold FROM order_items GROUP BY product_id',

# Subqueries

#36
f'''SELECT product_id FROM (
    SELECT product_id, SUM(quantity) AS total_sold
    FROM order_items
    GROUP BY product_id
) t WHERE total_sold > 100''',

#37
f'''SELECT user_id FROM (
    SELECT user_id, SUM(total_amount) AS total_spent
    FROM orders
    GROUP BY user_id
) t WHERE total_spent > 1000''',

#38
f'SELECT user_id FROM reviews GROUP BY user_id HAVING COUNT(*) >= 3',

#39
f'SELECT product_id FROM products WHERE product_id NOT IN (SELECT DISTINCT product_id FROM reviews)',

#40
f'SELECT order_id FROM order_items GROUP BY order_id HAVING SUM(quantity) > 5',

# Date and time queries

#41
f'SELECT * FROM orders WHERE order_date >= DATE_SUB(CURRENT_DATE, 7)',

#42
f'SELECT MONTH(order_date) AS month, SUM(total_amount) AS revenue FROM orders GROUP BY MONTH(order_date)',

#43
f'SELECT * FROM users WHERE created_at >= DATE_SUB(CURRENT_DATE, 180)',

#44
f'SELECT order_date, COUNT(*) AS order_count FROM orders GROUP BY order_date',

#45
f'SELECT user_id, MIN(order_date) AS first_order, MAX(order_date) AS last_order FROM orders GROUP BY user_id',

# Performance and optimization queries

#46
f'SELECT * FROM orders ORDER BY total_amount DESC LIMIT 5',

#47
f'''SELECT AVG(item_count) FROM (
    SELECT order_id, COUNT(*) AS item_count FROM order_items GROUP BY order_id
) t''',

#48
f'SELECT category, MAX(price) AS max_price FROM products GROUP BY category',

#49
f'''SELECT product_id, 
       (COUNT(CASE WHEN rating = 5 THEN 1 END) * 100.0 / COUNT(*)) AS percentage_5_star 
FROM reviews 
GROUP BY product_id''',

#50
f'''SELECT DISTINCT o.user_id
FROM orders o
LEFT JOIN reviews r ON o.user_id = r.user_id
WHERE r.user_id IS NULL''',

]
