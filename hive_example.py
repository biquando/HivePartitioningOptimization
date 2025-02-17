from pyhive import hive
conn = hive.Connection(host="localhost", port=10000)

cursor = conn.cursor()
cursor.execute("DROP TABLE student")
cursor.execute("CREATE TABLE student(name STRING, age INT)")
cursor.execute("INSERT INTO student VALUES('Alice', 20),('Bob', 21),('Charlie', 22)")
cursor.execute("SELECT * from student WHERE age > 20")
for result in cursor.fetchall():
    print(result)
