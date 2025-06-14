# Databricks notebook source
# DBTITLE 1,Longest Substring Without Repeating Characters
class Solution(object):
    def lengthOfLongestSubstring(self, s):
        """
        :type s: str
        :rtype: int
        """

        charSet=set()

        l=0
        res=0
        for r in range(len(s)):
            while s[r] in charSet:
                charSet.remove(s[l])
                l+=1
            charSet.add(s[r])
            res=max(res, r-l+1)
        return res
        

# COMMAND ----------

# DBTITLE 1,Longest Repeating Character Replacement
class Solution(object):
    def characterReplacement(self, s, k):
        """
        :type s: str
        :type k: int
        :rtype: int
        """
        count={}
        l=0
        res=0

        for r in range(len(s)):
            count[s[r]]=1+ count.get(s[r], 0)

            while (r-l+1) - max(count.values()) > k:
                count[s[l]]-=1
                l+=1

            res=max(res, r-l+1)
        
        return res

# COMMAND ----------

# DBTITLE 1,Fizz Buzz
class Solution(object):
    def fizzBuzz(self, n):
        """
        :type n: int
        :rtype: List[str]
        """
        res=[]
        for i in range(1, n+1):
            if i%3==0 and i%5==0:
                res.append("FizzBuzz")
            elif i%3==0:
                res.append("Fizz")
            elif i%5==0:
                res.append("Buzz")
            else:
                res.append(str(i))
            
        return res

# COMMAND ----------

# DBTITLE 1,Longest Common Prefix
class Solution(object):
    def longestCommonPrefix(self, strs):
        """
        :type strs: List[str]
        :rtype: str
        """

        if not strs:
            return ""
        
        # Start with the first string as the prefix
        prefix = strs[0]
        
        # Compare the prefix with each string in the list
        for s in strs[1:]:
            # Reduce the prefix until it matches the start of the string
            while not s.startswith(prefix):
                prefix = prefix[:-1]  # Remove the last character
                if not prefix:  # If prefix becomes empty
                    return ""
        
        return prefix

# COMMAND ----------

# DBTITLE 1,Valid Anagram
class Solution(object):
    def isAnagram(self, s, t):
        """
        :type s: str
        :type t: str
        :rtype: bool
        """

        if len(s) != len(t):
            return False
        counterS, counterT= {}, {}
        for i in range(len(s)):
            counterS[s[i]]=1+counterS.get(s[i],0)
            counterT[t[i]]=1+counterT.get(t[i],0)
        
        for c in counterS:
            if counterS[c] != counterT.get(c,0):
                return False
        
        return True
        

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

# DBTITLE 1,Valid Palindorme
class Solution(object):
    def isPalindrome(self, s):
        """
        :type s: str
        :rtype: bool
        """

        s= re.sub("[^a-z|^0-9]","", s.lower() )
        
        l, r = 0, len(s)-1

        while l < r:
            if s[l] != s[r]:
                return False
            l+=1
            r-=1

        return True

# COMMAND ----------

# DBTITLE 1,Longest Palindromic Substring
class Solution(object):
    def longestPalindrome(self, s):
        """
        :type s: str
        :rtype: str
        """
        res=""
        resLen=0

        for i in range(len(s)):
            #odd
            l, r= i, i
            while l>=0 and r <  len(s) and s[l] == s[r]:
                if (r-l+1) > resLen:
                    res=s[l:r+1]
                    resLen=r-l+1
                l-=1
                r+=1

            #even
            l, r= i, i+1
            while l>=0 and r <  len(s) and s[l] == s[r]:
                if (r-l+1) > resLen:
                    res=s[l:r+1]
                    resLen=r-l+1
                l-=1
                r+=1
        return res

# COMMAND ----------

# DBTITLE 1,Letter Combinations of a Phone Number
class Solution(object):
    def letterCombinations(self, digits):
        """
        :type digits: str
        :rtype: List[str]
        """
        phone_map = {
            "2": "abc", "3": "def", "4": "ghi", "5": "jkl",
            "6": "mno", "7": "pqrs", "8": "tuv", "9": "wxyz"
        }

        if not digits: return []

        output=[""]

        for d in digits:
            temp=[]
            for v in phone_map[d]:
                for o in output:
                    temp.append(o+v)
            output=temp
            
        return output


# COMMAND ----------

# DBTITLE 1,Palindromic Substrings
class Solution(object):
    def countSubstrings(self, s):
        """
        :type s: str
        :rtype: int
        """
        output = 0
        N=len(s)

        for i in range(len(s)):
            l, r = i, i
            while l >= 0 and r< N and s[l]==s[r]:
                output+=1
                l-=1
                r+=1

            l, r = i, i+1
            while l >= 0 and r< N and s[l]==s[r]:
                output+=1
                l-=1
                r+=1
        return output
