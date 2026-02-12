import pandas as pd
import matplotlib.pyplot as plt
import seaborn as se
import mysql.connector
import numpy as np

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='582008',
    database='ecommerce'
)
cursor = conn.cursor()

# Basic Questions

# 1. List all unique cities where customers are located

query1 = """ Select distinct customer_city from customers"""

cursor.execute(query1)
data1 = cursor.fetchall()
print(data1)


# 2. Count the number of orders placed in 2017
query2 = """ Select count(order_id) as Total_Orders from orders
where year(order_purchase_timestamp) = 2017 """
cursor.execute(query2)
data2 = cursor.fetchall()
print(data2)


# 3. Find the total sales per category
query3 = """ Select products.product_category , sum(payments.payment_value) as total_sales from products
join order_items
on products.product_id = order_items.product_id
join payments
on order_items.order_id = payments.order_id
group by products.product_category """
cursor.execute(query3)
data3 = cursor.fetchall()
print(data3)


# 4. Calculate the percentage of orders that were paid in installments

query4 = """select sum(case when payments.payment_installments >= 1 then 1 else 0 end)/count(*)*100 from payments;
"""
cursor.execute(query4)
data4 = cursor.fetchall()
print(data4)



# 5.Count the number of customers from each state. 

query5 = """Select customers.customer_state , count(customers.customer_unique_id) as Total_Customer from customers
group by customers.customer_state"""
cursor.execute(query5)
data5 = cursor.fetchall()
print(data5)


df = pd.DataFrame(data5 , columns= ["State", "Customers"])
print(df)
plt.bar(df["State"] , df["Customers"])
plt.show()




