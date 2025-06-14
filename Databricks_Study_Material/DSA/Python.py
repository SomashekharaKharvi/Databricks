# Databricks notebook source
from collections import Counter

str1 = "peter piper picked a peck of pickled peppers"
freq = Counter(str1)

for i in str1:
    print(i, freq[i])

# COMMAND ----------

s1 = "helloO"
dict = {}
for i in s1:
  keys=lower(dict.keys())
  if i.lower() in keys:
    dict[i]+=1
  else:
    dict[i]=1

print(dict)

# COMMAND ----------

str1 = "I wish I wish with all my heart to fly with dragons in a land apart"

dict1={}
words=str1.split()
for i in words:
  keys=dict1.keys()
  if i in keys:
    dict1[i]+=1
  else:
    dict1[i]=1

print(dict1)

# COMMAND ----------

best_char = sorted(dict1.items(), key=lambda x: x[1])[-1][0]
best_char

# COMMAND ----------

lst=[1,2,3,4,5,6,7,8,9]
j=0
for i in lst:
  j=j+i

print(j)

# COMMAND ----------

tuples_lst = [('Beijing', 'China', 2008), ('London', 'England', 2012), ('Rio', 'Brazil', 2016, 'Current'), ('Tokyo', 'Japan', 2020, 'Future')]
l=[]
for i in tuples_lst:
  l.append(i[1])

print(l)


# COMMAND ----------

gold = {'USA':31, 'Great Britain':19, 'China':19, 'Germany':13, 'Russia':12, 'Japan':10, 'France':8, 'Italy':8}
for i in gold.items():
  print(i[1])

# COMMAND ----------

x = [1, 3, 4, 5, 6, 7, 3]
sub=[]
x=(num for num in x)
num=next(x,7)
while num!=7:
  sub.append(num)
  num=next(x,7)

print(sub)

# COMMAND ----------

top_three = []
medals = {'Japan':41, 'Russia':56, 'South Korea':21, 'United States':121, 'Germany':42, 'China':70}
ks=medals.keys()
def get(d,k):
  return d[k]
res=sorted(ks, key= lambda x: get(medals, x), reverse=True)[:3]
print(res)

# COMMAND ----------

print(sorted(medals, reverse=True)[:3])

# COMMAND ----------

ids = [17573005, 17572342, 17579000, 17570002, 17572345, 17579329]

def last_four(x):
  return str(x)[-4:]
last_four(ids)

sorted(ids, key= lambda x: last_four(x))

# COMMAND ----------

ids = [17573005, 17572342, 17579000, 17570002, 17572345, 17579329]

def last_four(x):
    
    return (str(x)[-4:])
last_four(ids)

sorted_ids = sorted(ids, key=last_four )
print(sorted_ids)

# COMMAND ----------

input_string = "Hello, World!"
str=""
for i in input_string:
  str= i + str

print(str)

# COMMAND ----------

print(input_string[::-1])

# COMMAND ----------

add= lambda x, y : (x*x+y)
add(1,2)

# COMMAND ----------

def demo(fx):
  def hx(*awrg, **kwarg):
    print("This is the start")
    fx(*awrg, **kwarg)
    print("this is the end")
  return hx


@demo
def new(a,b):
  print( a*b)

new(10,3)



# COMMAND ----------

ids = [17573005, 17572342, 17579000, 17570002, 17572345, 17579329]

l=( i+1 for i in ids)
type(l)
for i in l:
  print (i)

# COMMAND ----------

def gen():
  for i in range(10):
    i=i*i
    yield i

n=gen()
# print(next(n))
# print(next(n))
for i in n:
  print(i)

# COMMAND ----------

class Employee:
  def __init__(self, name, salary):
    self.name=name
    self.salary=salary

  def add(self, val):
    self.salary= self.salary*val

  @staticmethod
  def yes():
    print("yessssss")

suraj = Employee('suraj',50000)

suraj.add(1.5)
print(suraj.salary)

Employee.yes()

class new_class(Employee):
  def __init__(self, name, salary, city):
    super().__init__(name, salary)
    self.city=city


nam=new_class('Nam', 50000,'kdpr')
print(nam.salary)
nam.add(2)
print(nam.salary)

# COMMAND ----------


