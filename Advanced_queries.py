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

# Advanced Questions

# 1.Calculate the moving average of order values for each customer over their order history

query11 = """select customer_id ,order_purchase_timestamp , payment_value , 
avg(payment_value) over(partition by customer_id order by order_purchase_timestamp rows between 2 preceding and current row) as mov_avg 
from
(select orders.customer_id , orders.order_purchase_timestamp , payments.payment_value from payments
join orders  
on payments.order_id = orders.order_id) as a;"""
cursor.execute(query11)
data11 = cursor.fetchall()
# print(data11)
df11 = pd.DataFrame(data11 , columns= ["Customer_id" , "time" , "payment" , "Average"])
print(df11)



# 2. Calculate the cumulative sales per month for each year

query12 = """select years , months ,payments, sum(payments) over(order by years , months)  Cum 
from
(select year(orders.order_purchase_timestamp) as years , month(orders.order_purchase_timestamp) as months
, round(sum(payments.payment_value),2) as payments from payments 
join orders
on payments.order_id = orders.order_id 
group by years , months order by years , months) as a;"""
cursor.execute(query12)
data12 = cursor.fetchall()
# print(data12)
df12 = pd.DataFrame(data12 , columns= ["year" , "months" , "payment" , "Cum"])
print(df12)


# 3. Calculate the year-over-year growth rate of total sales

query13 ="""select years , sales , round(((sales - lag(sales , 1) over(order by years)) / lag(sales , 1) over(order by years)),2) * 100 as Growth_rate from 
(select year(orders.order_purchase_timestamp) as years , round(sum(payments.payment_value),2) as sales from payments
join orders
on payments.order_id = orders.order_id
group by years order by years) as a;"""
cursor.execute(query13)
data13 = cursor.fetchall()
# print(data13)
df13 = pd.DataFrame(data13 , columns= ["Years","Sales","Growth_rate"])
print(df13)


# 4. Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.

query14 = """with a as (select customers.customer_id , min(orders.order_purchase_timestamp) as first_sale from customers
join orders 
on customers.customer_id  = orders.customer_id 
group by customers.customer_id) ,

b as (select a.customer_id , count(distinct orders.order_purchase_timestamp) as next_order from a
join orders
on a.customer_id  = orders.customer_id
and orders.order_purchase_timestamp > first_order
and orders.order_purchase.timestamp < date_add(first_order ,interval 6 month)
group by a.customer_id)

select 100 * (count(distinct a.customer_id)/count(distinct b.customer_id))
from a left join b 
on a.customer_id = b.customer_id;"""
cursor.execute(query13)
data14 = cursor.fetchall()
df14 = pd.DataFrame(data14)
print(df14)


# 5. Identify the top 3 customers who spent the most money in each year

query15 = """select years , customer_id , Payment , d_rank from
(select year(orders.order_purchase_timestamp) as years ,  orders.customer_id , sum(payments.payment_value) as Payment ,
dense_rank() over( partition by year(orders.order_purchase_timestamp) order by sum(payments.payment_value) desc) d_rank
from orders
join payments
on ecommerce.orders.order_id = payments.order_id
group by year(orders.order_purchase_timestamp) , orders.customer_id) as a
where d_rank <= 3;"""
cursor.execute(query15)
data15 = cursor.fetchall()
df15 = pd.DataFrame(data15 , columns= ["Year" , "Customer_ID" , "Payment" , "Rank"])
print(df15)

se.barplot(data = df15,x = "Customer_ID" , y = "Payment" ,  hue = "Year" )
plt.xticks(rotation = 90)
plt.show()