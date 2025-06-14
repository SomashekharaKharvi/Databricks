# Databricks notebook source
# DBTITLE 1,Number of 1 Bits
def hammingweight(n):
    res = 0
    while n:
        res = res + n % 2  # Add 1 to res if the least significant bit is 1
        n = n >> 1         # Right shift n to process the next bit
    return res
  
print(hammingweight(11))  # Output: 3 (binary representation: 1011)
print(hammingweight(128))  # Output: 1 (binary representation: 10000000)
print(hammingweight(7))    # Output: 3 (binary representation: 111)
print(hammingweight(0))    # Output: 0 (binary representation: 0)

# COMMAND ----------

# DBTITLE 1,Missing Number
def missingNumber(nums):
  res=len(nums)

  for i in range(len(nums)):
    res+= (i-nums[i])

  return res

print(missingNumber([3, 0, 1]))  # Output: 2
print(missingNumber([0, 1]))     # Output: 2
print(missingNumber([9, 6, 4, 2, 3, 5, 7, 0, 1]))  # Output: 8

# COMMAND ----------


