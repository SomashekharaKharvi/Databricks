# Databricks notebook source
class HashTable:
    def __init__(self, size = 7):
        self.data_map = [None] * size
      
    def __hash(self, key):
        my_hash = 0
        for letter in key:
            my_hash = (my_hash + ord(letter) * 23) % len(self.data_map)
        return my_hash  

    def print_table(self):
        for i, val in enumerate(self.data_map): 
            print(i, ": ", val)
    
    def set_item(self, key, value):
        index = self.__hash(key)
        if self.data_map[index] == None:
            self.data_map[index] = []
        self.data_map[index].append([key, value])
    
    def get_item(self, key):
        index = self.__hash(key)
        if self.data_map[index] is not None:
            for i in range(len(self.data_map[index])):
                if self.data_map[index][i][0] == key:
                    return self.data_map[index][i][1]
        return None

    # def keys(self):
    #     all_keys = []
    #     for i in range(len(self.data_map)):
    #         if self.data_map[i] is not None:
    #             for j in self.data_map[i]:
    #                 all_keys.append(self.data_map[i][j][0])
    #     return all_keys
        
    def keys(self):
        all_keys = []
        for i in range(len(self.data_map)):
            if self.data_map[i] is not None:
                for pair in self.data_map[i]:  # Use 'pair' instead of 'j'
                    all_keys.append(pair[0])  # Access the first element of the pair
        return all_keys
            

my_hash_table = HashTable()

my_hash_table.set_item('bolts', 1400)
my_hash_table.set_item('washers', 50)
my_hash_table.set_item('lumber', 70)

print(my_hash_table.keys())



"""
    EXPECTED OUTPUT:
    ----------------
    ['bolts', 'washers', 'lumber']

"""

# COMMAND ----------

# DBTITLE 1,Item in common
def item_in_commom(list1, list2):
  d={}
  for i in list1:
    d[i]=True

  for j in list2:
    if j in d.keys():
      return  True
  None

l1=[2, 3, 6]
l2=[5, 4, 8]
print(item_in_commom(l1,l2))

# COMMAND ----------

# DBTITLE 1,find_duplicates
def find_duplicates(nums):
    num_counts = {}
    for num in nums:
        num_counts[num] = num_counts.get(num, 0) + 1
 
    duplicates = []
    for num, count in num_counts.items():
        if count > 1:
            duplicates.append(num)
 
    return duplicates
  
print ( find_duplicates([1, 2, 3, 4, 5]) )
print ( find_duplicates([1, 1, 2, 2, 3]) )
print ( find_duplicates([1, 1, 1, 1, 1]) )
print ( find_duplicates([1, 2, 3, 3, 3, 4, 4, 5]) )
print ( find_duplicates([1, 1, 2, 2, 2, 3, 3, 3, 3]) )
print ( find_duplicates([1, 1, 1, 2, 2, 2, 3, 3, 3, 3]) )
print ( find_duplicates([]) )

# COMMAND ----------

# DBTITLE 1,first_non_repeating_char
def first_non_repeating_char(string):
    char_counts = {}
    for char in string:
        char_counts[char] = char_counts.get(char, 0) + 1
    for char in string:
        if char_counts[char] == 1:
            return char
    return None

# COMMAND ----------

# DBTITLE 1,Anagram
def group_anagrams(strings):
    anagram_groups = {}
    for string in strings:
        canonical = ''.join(sorted(string))
        if canonical in anagram_groups:
            anagram_groups[canonical].append(string)
        else:
            anagram_groups[canonical] = [string]
    return list(anagram_groups.values())
  
print("1st set:")
print( group_anagrams(["eat", "tea", "tan", "ate", "nat", "bat"]) )

print("\n2nd set:")
print( group_anagrams(["abc", "cba", "bac", "foo", "bar"]) )

print("\n3rd set:")
print( group_anagrams(["listen", "silent", "triangle", "integral", "garden", "ranged"]) )

# COMMAND ----------

# DBTITLE 1,two_sum
def two_sum(nums, target):
    num_map = {}
    for i, num in enumerate(nums):
        complement = target - num
        if complement in num_map:
            return [num_map[complement], i]
        num_map[num] = i
    return []

print(two_sum([5, 1, 7, 2, 9, 3], 10))  
# print(two_sum([4, 2, 11, 7, 6, 3], 9))  
# print(two_sum([10, 15, 5, 2, 8, 1, 7], 12))  
# print(two_sum([1, 3, 5, 7, 9], 10))  
# print ( two_sum([1, 2, 3, 4, 5], 10) )
# print ( two_sum([1, 2, 3, 4, 5], 7) )
# print ( two_sum([1, 2, 3, 4, 5], 3) )
# print ( two_sum([], 0) )


# COMMAND ----------

# DBTITLE 1,HT: Subarray Sum ( ** Interview Question)
def subarray_sum(nums, target):
    sum_index = {0: -1}
    current_sum = 0
    for i, num in enumerate(nums):
        current_sum += num
        if current_sum - target in sum_index:
            return [sum_index[current_sum - target] + 1, i]
        sum_index[current_sum] = i
    return []

nums = [1, 2, 3, 4, 5]
target = 9
print ( subarray_sum(nums, target) )

nums = [-1, 2, 3, -4, 5]
target = 0
print ( subarray_sum(nums, target) )

nums = [2, 3, 4, 5, 6]
target = 3
print ( subarray_sum(nums, target) )

nums = []
target = 0
print ( subarray_sum(nums, target) )

# COMMAND ----------

# DBTITLE 1,Set: Remove Duplicates ( ** Interview Question)
def remove_duplicates(my_list): 
    # Convert the list to a set and then back to a list to remove duplicates 
    new_list = list(set(my_list)) 
    return new_list

# COMMAND ----------

def has_unique_chars (string): 
    # Convert the list to a set and then back to a list to remove duplicates 
    if len(sorted(string)) == len(list(set(sorted(string))) ):
      return True
    else:
      return False
    
# def has_unique_chars(string):
#     char_set = set()
#     for char in string:
#         if char in char_set:
#             return False
#         char_set.add(char)
#     return True

print(has_unique_chars('abcdefg')) # should return True
print(has_unique_chars('hello')) # should return False
print(has_unique_chars('')) # should return True
print(has_unique_chars('0123456789')) # should return True
print(has_unique_chars('abacadaeaf')) # should return False

# COMMAND ----------

# DBTITLE 1,Set: Find Pairs ( ** Interview Question)
def find_pairs(arr1, arr2, target):
    set1 = set(arr1)
    pairs = []
    for num in arr2:
        complement = target - num
        if complement in set1:
            pairs.append((complement, num))
    return pairs

arr1 = [1, 2, 3, 4, 5]
arr2 = [2, 4, 6, 8, 10]
target = 7

pairs = find_pairs(arr1, arr2, target)
print (pairs)

# COMMAND ----------

# DBTITLE 1,longest_consecutive_sequence
def longest_consecutive_sequence(nums):
    num_set = set(nums)
    longest_sequence = 0
    
    for num in nums:
        if num - 1 not in num_set:
            current_num = num
            current_sequence = 1
            
            while current_num + 1 in num_set:
                current_num += 1
                current_sequence += 1
            
            longest_sequence = max(longest_sequence, current_sequence)
    
    return longest_sequence
  
print( longest_consecutive_sequence([100, 4, 200, 1, 3, 2]) )

