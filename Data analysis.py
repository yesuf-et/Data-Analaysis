#!/usr/bin/env python
# coding: utf-8

# # Question 1

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName("emp_data").getOrCreate()


# In[3]:


data =(['Abel','Sales', 'Deri Dawa', 90000,34,10000],
       ["Mesfin","Sales","Deri Dawa",86000,56,20000],
       ["Tesfaw","Sales","Gonder", 81000, 30, 23000],
       ["Iwunete", "Finance","Jijiga", 90000, 49, 23000],
       ["Kefele", "Finance",  "Gonder", 99000, 40,24000],
       ["Palit",   "Finance", "Dire Dawa", 83000, 36, 19000],
       ["Kebede",   "Finance",   "Gonder", 79000, 53, 15000],
       ["Ashebir", "Marketing",   "Jijiga", 80000, 65, 18000],
       ["Melkamu", "Marketing",   "Gonder", 91000, 50, 21000],
       ["Betelihem","Sales",    "Adama",  4000, 26, 14000],
       ["Gessese", "Finance", "Dire Dawa",  7000, 43, 12000],
       ["Melat",   "Sales",    "Adama", 78000, 39, 31000]
      )


# In[4]:


column =(["Employee_name","department","state","salary","age","bonus"])


# In[5]:


dfr2= spark.createDataFrame(data,column)


# In[7]:


type(dfr2)


# In[8]:


dfr2 = spark.read.csv("c:\data\emp_data.csv", header=True, inferSchema=True)


# In[9]:


dfr2.show()


# 
# # Question 2

# # a. function returns unique department and shows count of  sales department?

# In[10]:


dfr2.groupBy('department').sum('bonus').show()


# In[13]:


dfr2.groupBy(dfr2['department']).count().filter((dfr2["department"]=="Sales")).show()


# # function

# In[14]:


def unique_department(data_frame):

    unique_dept = data_frame.groupBy(data_frame['department']).count()
    count_of_sales = data_frame.groupBy(data_frame['department']).count().filter((data_frame["department"]=="Sales"))
    return unique_dept,count_of_sales;


# In[18]:


def abc(data_frame):
    data_frame.groupBy('state').avg('salary')
    return unique_dept,count_of_sales;


# # b. function returns aggregate per state

# # b i  # AVG salary in Dere Dawa

# In[19]:


dfr2.groupBy('state').avg('salary').filter((dfr2["state"]=="Dire Dawa")).show()


# # b ii,  total bonus in Gijiga

# In[20]:


dfr2.groupBy('state').agg({'bonus':'sum'}).filter((dfr2["state"]=="Jijiga")).show()


# # b iii,  mininum salary in Gonder  

# In[21]:


dfr2.groupBy('state').agg({'salary':'min'}).filter((dfr2["state"]=="Gonder")).show()


# # b iV, states with maximum age

# In[22]:


max_age = dfr2.groupBy('state').agg({'age':'max'}).tail(1)


# # function

# In[23]:


def agg_per_state(data_frame):
    dire = data_frame.groupBy('state').avg('salary').filter((data_frame["state"]=="Dire Dawa"))
    jij = data_frame.groupBy('state').agg({'bonus':'sum'}).filter((data_frame["state"]=="Jijiga"))
    gon = data_frame.groupBy('state').agg({'salary':'min'}).filter((data_frame["state"]=="Gonder"))
    max_age = data_frame.groupBy('state').agg({'age':'max'}).show()
    return dire, jij,gon, max_age


# # c, filter employees who's age > 40

# In[26]:


dfr2.filter((dfr2["age"]>=40)).show(3)


# # d. Summarizing

# In[27]:


summary = dfr2.groupBy('department','state').agg({'salary':'sum','employee_name':'count','bonus':'avg'}).show()


# # Question 3

# In[ ]:


from pyspark.sql import SparkSession

# create a SparkSession object
spark = SparkSession.builder     .appName("Insert data to MySQL")     .getOrCreate()
# create a DataFrame with the data to be inserted
data = summary
df = spark.createDataFrame(data, ["state", "department", "total_salary", "total_employees","average_bonus"])


# In[ ]:



df.write     .format("jdbc")     .option("url", "jdbc:mysql://localhost/emp_data_container")     .option("driver", "com.mysql.jdbc.Driver")     .option("dbtable", "mytable")     .option("user", "root")     .option("password", "")     .mode("append")     .save()
spark.stop()


# In[290]:


import matplotlib.pyplot as plt

x = dfr2.agg({'salary':'sum'}).show()
y = dfr2.agg({'bonus':'avg'}).show()
plt.scatter(x, y)
plt.show()


# In[ ]:




