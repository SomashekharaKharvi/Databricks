# Databricks notebook source
# DBTITLE 1,climbStairs
def climbStairs(n):

    # 1 2 3 4 5 6  7
    # 1 2 3 5 8 13 21
    if n <= 2:
        return 2
    
    prev1=1
    prev2=2
    curr=0
    for i in range(2, n):
        curr=prev1+prev2
        prev1=prev2
        prev2=curr

    return curr

# Test the function
print(climbStairs(5))  # Output: 8

# COMMAND ----------

def generate(numRows):
  res=[[1]]

  for i in range(numRows-1):
    temp=[0]+res[-1]+[0]
    row=[]
    for j in range(len(res[-1])+1):
      row.append(temp[j]+ temp[j+1])
    res.append(row)
   
  return res
print(generate(5))

