# Databricks notebook source
# MAGIC %md LISTS

# COMMAND ----------

def func():
    pattern='abba'
    string='dog cat cat dog'
    words = string.split(" ")
    if len(pattern) != len(words):
        print("false")
    
    # Create two dictionaries for mapping
    char_to_word = {}
    word_to_char = {}
    
    for char, word in zip(pattern, words):
        print(char, word)
        if char in char_to_word:
            if char_to_word[char]!= word:
                return False
        else:
            char_to_word[char]=word
        
        if word in word_to_char:
           if word_to_char[word]!= char:
             return False
            
        else:
           word_to_char[word]=char
        
    return True
    
print(fuc())

# COMMAND ----------

# DBTITLE 1,Prime Number
def prime_n(n):
    if n<2:
        return False
    for i in range(2,n):
        if n%i==0:
            return False
    return True
        
for i in range(0, 200):
    if prime_n(i):
        print(i)

# COMMAND ----------

# DBTITLE 1,Sort List
lst=[1,2,34,53,64,1,42,5,64,2,2432]

for i in range(len(lst)):
    for j in range(i+1, len(lst)):
        if lst[i]>=lst[j]:
           lst[i], lst[j] = lst[j], lst[i]

print(lst)

# sorted(lst)

# COMMAND ----------

# DBTITLE 1,Merge Two Sorted Array
def merge_sorted_arrays(arr1, arr2):
    res=[]
    i,j=0,0
    
    while i < len(arr1) and j < len(arr2):
        if arr1[i]<arr2[j]:
            res.append(arr1[i])
            i+=1
        
        else:
            res.append(arr2[j])
            j+=1
            
    while i < len(arr1):
        res.append(arr1[i])
        i+=1
    
    while j < len(arr2):
        res.append(arr2[j])
        j+=1
    
    return res
    
arr1 = [1, 3, 5]
arr2 = [2, 4, 6]
result = merge_sorted_arrays(arr1, arr2)
print(result)

# COMMAND ----------

# DBTITLE 1,Febonaci
def febonaci(n):
    feb=[0, 1]
    
    for i in range(n):
        feb.append(feb[-1]+feb[-2])
        
    return feb
    
print(febonaci(10))

# COMMAND ----------

# DBTITLE 1,Reverse List Manually
# nums.reverse()
# print(nums)
# print(nums[::-1])

def reverse_lst(nums):
    end=len(nums)-1
    start=0
    while start < end:
        nums[start], nums[end] = nums[end] , nums[start]
        start+=1
        end-=1
        
    return nums
nums=[0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]    
print(reverse_lst(nums))

# COMMAND ----------

# DBTITLE 1,Reverse / Palindrome
string='Suraj'
res=''
for c in string:
    res=c+res
    
print(res)

# COMMAND ----------

# DBTITLE 1,duplicates in LIst
nums=[0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]

nums=[0, 1, 1, 2,2, 3, 5, 8, 13, 21, 34, 55, 89]

seen=set()
duplicate=set()

for i in nums:
    if i in seen:
        duplicate.add(i)
    else:
        seen.add(i)
        
print(duplicate)

# COMMAND ----------

# DBTITLE 1,Count Charcters in String
string="suraj is good boy"

from collections import Counter
print(Counter(string))

num_map={}
for c in string:
    if c in num_map:
        num_map[c]+=1
    else:
        num_map[c]=1
        
print(num_map)

# COMMAND ----------

# DBTITLE 1,Join Multiple String using Space
word1='Suraj'
word2='Kharvi'

print(' '.join((word1, word2, 'Great')))

# COMMAND ----------

# DBTITLE 1,Numbers in String
word='Su1234ra4j'
nums=[]
for c in word:
    if c.isdigit():
        nums.append(c)
    
print(''.join(nums))

# COMMAND ----------

# DBTITLE 1,Remove Duplicate, Keep Only first Occurance
string='ShivaVishnuInfoys'

res=''
seen=set()

for c in string:
    if c not in seen:
        res+=c
        seen.add(c)
        
print(res)

# COMMAND ----------

# DBTITLE 1,Factorial

def fact(n):
    fact=1
    if n==0:
        print('ivalid')
    elif n==1:
        return 1

    else:
        for i in range(2,n+1):
           fact= fact*i
           
    return fact
    
print(fact(5))


# with recursive

def fact_r(n):
    if n==1 or n==0:
        return 1
    else:
       fact=n*fact_r(n-1)
    
    return fact

print(fact_r(5))   
    

# COMMAND ----------

# DBTITLE 1,First Non Repeating Chracter
def first_non_repeating_char(word):
    
    num_map={}
    
    for c in word:
        num_map[c]=num_map.get(c, 0)+1
    
    for c in num_map:
        if num_map[c]==1:
            return c
     
print(first_non_repeating_char("swiss"))

# COMMAND ----------

# DBTITLE 1,Moving all Zeros to end of Listt
def move_zeros_easy(arr):
    result = []

    # Add all non-zero elements first
    for num in arr:
        if num != 0:
            result.append(num)

    # Count zeros and add them at the end
    zero_count = arr.count(0)
    result.extend([0] * zero_count)

    return result


# COMMAND ----------

# DBTITLE 1,odd group
l=[1,2,3 , 4, 5, 6, 7, 8]
odd=[]

even=[]
for i in l:
  if i%2==0:
    even.append(i)
  else:
    odd.append(i)
res=[]
for i in range(len(odd)):
  res.append((odd[i], even[i]),)

print(res)

# COMMAND ----------

# DBTITLE 1,two sum
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

# DBTITLE 1,Remove Duplicates from Sorted Array
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

# COMMAND ----------

# DBTITLE 1,Valid Parentheses
class Solution(object):
    def isValid(self, s):
        """
        :type s: str
        :rtype: bool
        """
        stack=[]

        closetoOpen={")":"(", "]":"[", "}":"{"}

        for c in s:
            if c in closetoOpen:
                if stack and stack[-1]==closetoOpen[c]:
                    stack.pop()
                else:
                    return False
            else:
                stack.append(c)

        return True if not stack  else False



# COMMAND ----------

list_A = [2, 1, 2, 3, 4, 5, 9]
list_B = [5, 4, 5, 6, 7, 8]

res=[]

for i in list_A:
  if i not in list_B:
    res.append(i)

for i in list_B:
  if i not in list_A:
    res.append(i)

print(set(res))

print(set(list_A).symmetric_difference(list_B))

# COMMAND ----------

# DBTITLE 1,find common elements in list
list1 = [1, 2, 3, 4]
list2 = [3, 4, 5, 6]

common = list(set(list1) & set(list2))
print(common)  # Output: [3, 4]

or 

list1 = [1, 2, 3, 4]
list2 = [3, 4, 5, 6]

common = [x for x in list1 if x in list2]
print(common)  # Output: [3, 4]

# COMMAND ----------

# DBTITLE 1,find uncommon elements in list
list1 = [1, 2, 3, 4]
list2 = [3, 4, 5, 6]

uncommon = list(set(list1) ^ set(list2))  # ^ is symmetric difference
print(uncommon)  # Output: [1, 2, 5, 6]


Or 

list1 = [1, 2, 3, 4]
list2 = [3, 4, 5, 6]

uncommon = [x for x in list1 + list2 if (x not in list1 or x not in list2)]
print(uncommon)  # Output: [1, 2, 5, 6]


# COMMAND ----------

# DBTITLE 1,Second largest number
# Sort the list and select lst[-2]

# COMMAND ----------

# DBTITLE 1,Missing number from 1 to n python Copy Edit
lst = [1, 2, 4, 5]
n = 5
missing = (n * (n + 1)) // 2 - sum(lst)
print(missing)
# Output: 3


# COMMAND ----------

# DBTITLE 1,Group anagram
from collections import defaultdict
words = ["eat", "tea", "tan", "ate", "nat", "bat"]
anagrams = defaultdict(list)
for word in words:
    key = ''.join(sorted(word))
    anagrams[key].append(word)

print(list(anagrams.values()))
# Output: [['eat', 'tea', 'ate'], ['tan', 'nat'], ['bat']]


# COMMAND ----------

# MAGIC %md STRINGS

# COMMAND ----------

# DBTITLE 1,Compress a String
def compress(s):
    result = ""
    count = 1
    for i in range(1, len(s)):
        if s[i] == s[i-1]:
            count += 1
        else:
            result += s[i-1] + str(count)
            count = 1
    result += s[-1] + str(count)
    return result

print(compress("aaabbccc"))  # Output: a3b2c3


# COMMAND ----------

# DBTITLE 1,Find All Permutations of a String
from itertools import permutations

s = "abc"
perm = permutations(s)
for p in perm:
    print(''.join(p))
# Output: abc, acb, bac, bca, cab, cba


# COMMAND ----------

# DBTITLE 1,Check if One String is Rotation of Another
def is_rotation(s1, s2):
    return len(s1) == len(s2) and s2 in (s1 + s1)

print(is_rotation("abcde", "deabc"))  # Output: True


# COMMAND ----------

# DBTITLE 1,Longest Word in a Sentence
sentence = "The quick brown fox"
words = sentence.split()
print(max(words, key=len))  # Output: "quick"

