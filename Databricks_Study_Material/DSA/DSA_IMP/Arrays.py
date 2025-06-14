# Databricks notebook source
# DBTITLE 1,Two Sum
def two_sum(nums, target):
    num_map = {}
    for i, num in enumerate(nums):
        complement = target - num
        if complement in num_map:
            return [num_map[complement], i]
        num_map[num] = i
    return []

print(two_sum([5, 1, 7, 2, 9, 3], 10))  
print(two_sum([4, 2, 11, 7, 6, 3], 9))  
print(two_sum([10, 15, 5, 2, 8, 1, 7], 12))  
print(two_sum([1, 3, 5, 7, 9], 10))  
print ( two_sum([1, 2, 3, 4, 5], 10) )
print ( two_sum([1, 2, 3, 4, 5], 7) )
print ( two_sum([1, 2, 3, 4, 5], 3) )
print ( two_sum([], 0) )


# COMMAND ----------

# DBTITLE 1,Best Time to Buy and Sell
def maxProfit(prices):
  l, r = 0, 1
  maxP=0

  while r <len(prices):
    if prices[l]< prices[r]:
      profit=prices[r]-prices[l]
      maxP=max(maxP, profit)
    else:
      l=r
    r+=1
  return maxP

print(maxProfit([7, 1, 5, 3, 6, 4]))


# COMMAND ----------

# DBTITLE 1,Contains Duplicate
def containDuplicate(list):
  hashSet=set()
  for i in list:
    if i in hashSet:
      return True
    else:
      hashSet.add(i)
  return False

print(containDuplicate([1, 2, 3, 4]))

# COMMAND ----------

# DBTITLE 1,Contains Duplicate II
def containsNearByDuplicates(num, k):
  hashSet=set()
  l, r =0, 0
  for r in range(len(num)):
    if r-l > k:
      hashSet.remove(num[l])
      l+=1
    if num[r] in hashSet:
      return True
    hashSet.add(num[r])
  return False

print(containsNearByDuplicates([1, 2, 3, 1], 3))

# COMMAND ----------

# DBTITLE 1,Product of Array Except Self
class Solution(object):
    def productExceptSelf(self, nums):
        """
        :type nums: List[int]
        :rtype: List[int]
        """
        n= len(nums)

        ans= [1]* n

        for i in range(1,n):
            ans[i] = ans[i-1] * nums[i-1]
        
        right_product = 1

        for i in range(n-1 , -1, -1):
            ans[i]= right_product * ans[i]

            right_product*=nums[i]
            
        return ans

# COMMAND ----------

# DBTITLE 1,Maximum Subarray
class Solution(object):
    def maxSubArray(self, nums):
        """
        :type nums: List[int]
        :rtype: int
        """
    
        maxSubArray=nums[0]
        curSum=0

        for n in nums:
            if curSum < 0:
                curSum=0
            curSum+=n
            maxSubArray=max(curSum , maxSubArray)

        return maxSubArray

    
        

# COMMAND ----------

# DBTITLE 1,Maximum Product Subarray
class Solution(object):
    def maxProduct(self, nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        res = max(nums)
        curMin , curMax = 1, 1
        

        for n in nums:
            if n == 0:
                curMin, curMax = 1, 1  # Reset for zero
                continue
            temp= curMax * n
            curMax= max(n * curMax , n * curMin , n )
            curMin= max(temp , n * curMin , n )

            res= max(res, curMax)

        return res
        

# COMMAND ----------

# DBTITLE 1,Find Minimum in Rotated Sorted Array
class Solution(object):
    def findMin(self, nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        
        N= len(nums)
        l, r = 0, N-1

        while l < r:
            mid= (l+r)//2
            if nums[mid] > nums[r]:
                l=mid+1
            else:
                r=mid

        return nums[l]


# COMMAND ----------

# DBTITLE 1,Search in Rotated Sorted Array
class Solution(object):
    def search(self, nums, target):
        """
        :type nums: List[int]
        :type target: int
        :rtype: int
        """
        N=len(nums)
        l, r = 0 , N-1

        while l < r:
            mid=(l+r)//2
            if nums[mid] > nums[r]:
                l= mid +1
            else:
                r=mid
        pivot=l

        l , r = 0, N-1
        pi
        while l <= r:
            mid=(l+r)//2
            mid2=(mid+pivot)%N

            if nums[mid2] == target:
                return mid2
            elif nums[mid2] < target:
                l=mid+1
            else:
                r=mid-1
        return -1

# COMMAND ----------

# DBTITLE 1,Two Sum - II
class Solution(object):
    def twoSum(self, nums, target):
        """
        :type numbers: List[int]
        :type target: int
        :rtype: List[int]
        """
        N=len(nums)
        l, r = 0 , N-1

        while l < r:
            curSum = nums[l] + nums[r]

            if curSum > target:
                r-=1
            elif  curSum < target:
                l+=1
            else:
                return [l+1, r+1]


# COMMAND ----------

# DBTITLE 1,3 Sum
class Solution(object):
    def threeSum(self, nums):
        """
        :type nums: List[int]
        :rtype: List[List[int]]
        """

        res=[]
        nums.sort()

        for i, a in enumerate(nums):
            if i > 0 and a==nums[i-1]:
                continue
            
            l,r=i+1, len(nums)-1

            while l < r :
                threeSum = a + nums[l] + nums[r]

                if threeSum > 0 :
                    r-=1
                elif  threeSum < 0:
                    l+=1
                else:
                    res.append([a, nums[l], nums[r]])
                    l+=1
                    if nums[l] == nums[l-1] and l < r:
                        l+=1
        return res
            

# COMMAND ----------

# DBTITLE 1,Container With Most Water
class Solution(object):
    def maxArea(self, height):
        """
        :type height: List[int]
        :rtype: int
        """
        l, r= 0, len(height)-1
        area=0

        while l < r:
            area =max(area, (r-l)*(min(height[l], height[r])))

            if height[l] > height[r]:
                r-=1
            else:
                l+=1
        return area
        

# COMMAND ----------

# DBTITLE 1,Verifying an Alien Dictionary
class Solution(object):
    def isAlienSorted(self, words, order):
        """
        :type words: List[str]
        :type order: str
        :rtype: bool
        """

        ordInd={ c: i for i, c in enumerate(order)}

        for i in range(len(words)-1):
            w1, w2 = words[i], words[i+1]

            for j in range(len(w1)):
                if j==len(w2):
                    return False
                
                if w1[j]!=w2[j]:
                    if ordInd[w2[j]] < ordInd[w1[j]]:
                        return False
                    break
        return True
        

# COMMAND ----------

# DBTITLE 1,Remove Duplicates from Sorted Array
class Solution(object):
    def removeDuplicates(self, nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        l=1

        for r in range(1, len(nums)):
            if nums[r]!=nums[r-1]:
                nums[l]=nums[r]
                l+=1
        return l

# COMMAND ----------

# DBTITLE 1,Find First and Last Position of Element in Sorted Array
class Solution(object):
    def searchRange(self, nums, target):
        """
        :type nums: List[int]
        :type target: int
        :rtype: List[int]
        """
        if not nums:
            return [-1, -1]
        
        st, end = -1, -1
        N=len(nums)-1
        l, r = 0, N
        while l < r:
            mid=(l+r)//2
            if nums[mid]>=target:
                r=mid
            else:
                l=mid+1
        if l < N and nums[l]==target:
            st=l
        
        l, r = 0, N
        while l < r:
            mid=(l+r)//2
            if nums[mid]<=target:
                l=mid+1
            else:
                r=mid
            
        if nums[r-1]==target:
            end=r-1
        
        return [st, end]

        

# COMMAND ----------

# DBTITLE 1,romanToInt
def romanToInt(s):
    roman_map = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000
    }
    
    res=0

    for i in range(len(s)):
      if i+1 < len(s) and roman_map[s[i]] < roman_map[s[i+1]]:
        res-=roman_map[s[i]]
      else:
        res+=roman_map[s[i]]

    return res
  
print(romanToInt("III"))      # Output: 3
print(romanToInt("IV"))       # Output: 4
print(romanToInt("IX"))       # Output: 9
print(romanToInt("LVIII"))    # Output: 58
print(romanToInt("MCMXCIV"))  # Output: 1994

# COMMAND ----------

# DBTITLE 1,Merge  Array
def merge(l1, l2):
    res=[]
    i, j = 0, 0
    
    while i < len(l1) and j < len(l2):
        if l1[i] < l2[j]:
            res.append(l1[i])
            i+=1
        elif l1[i] > l2[j]:
                res.append(l2[j])
                j+=1 
        else:
            res.append(l1[i])
            res.append(l2[j])
            
            i+=1
            j+=1
            
            
    res.extend(l1[i:])
    res.extend(l2[j:])
    
    return res
    
# Example usage
# array1 = [1, 3, 5]
# array2 = [2, 4, 6]
# merged_sorted_array = merge(array1, array2)
# print(merged_sorted_array)      
print(merge([1, 2, 3, 0, 0, 0], [2, 5, 6]))

# COMMAND ----------

# DBTITLE 1,merge Sorted Array
def merge(nums1, nums2, m, n):
  last=m+n -1

  while m > 0 and n > 0:
    if nums1[m-1] > nums2[n-1]:
      nums1[last]=nums1[m-1]
      m-=1
    else:
      nums1[last]=nums2[n-1]
      n-=1
    last-=1
  
  while n>0:
    nums1[last]=nums2[n-1]
    last-=1
    n-=1

  return nums1

print(merge([1, 2, 3, 0, 0, 0], [2, 5, 6], 3, 3))

# COMMAND ----------

# DBTITLE 1,Majority Element
def majority_element(nums):
    count_map = {}
    n = len(nums)

    for num in nums:
        count_map[num] = count_map.get(num, 0) + 1
        if count_map[num] > n // 2:
            return num

    return -1

arr = [1, 1, 2, 1, 3, 5, 1]
print(majority_element(arr))  # Output: 1
