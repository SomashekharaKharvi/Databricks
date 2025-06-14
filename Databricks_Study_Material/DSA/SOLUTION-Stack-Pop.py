# Databricks notebook source
class Node:
    def __init__(self, value):
        self.value = value
        self.next = None
        

class Stack:
    def __init__(self, value):
        new_node = Node(value)
        self.top = new_node
        self.height = 1

    def print_stack(self):
        temp = self.top
        while temp is not None:
            print(temp.value)
            temp = temp.next

    def push(self, value):
        new_node = Node(value)
        if self.height == 0:
            self.top = new_node
        else:
            new_node.next = self.top
            self.top = new_node
        self.height += 1
        return True

    def pop(self):
        if self.height == 0:
            return None
        temp = self.top
        self.top = self.top.next
        temp.next = None
        self.height -= 1
        return temp
    

    

my_stack = Stack(4)
my_stack.push(3)
my_stack.push(2)
my_stack.push(1)

print('Stack before pop():')
my_stack.print_stack()

# print('\nPopped node:')
# print(my_stack.pop().value)

# print('\nStack after pop():')
# my_stack.print_stack()



"""
    EXPECTED OUTPUT:
    ----------------
    Stack before pop():
    1
    2
    3
    4

    Popped node:
    1

    Stack after pop():
    2
    3
    4

"""

# COMMAND ----------

# DBTITLE 1,Stack: Parentheses Balanced
# def is_balanced_parentheses(s):
#     stack = []
#     for char in s:
#         if char == '(':
#             stack.append(char)
#         elif char == ')':
#             if not stack:
#                 return False  # Unmatched closing parenthesis
#             stack.pop()  # Pop the matching opening parenthesis
#     return len(stack) == 0  # If stack is empty, parentheses are balanced

# ================================================================================================================================

class Stack:
    def __init__(self):
        self.stack_list = []

    def print_stack(self):
        for i in range(len(self.stack_list)-1, -1, -1):
            print(self.stack_list[i])

    def is_empty(self):
        return len(self.stack_list) == 0

    def peek(self):
        if self.is_empty():
            return None
        else:
            return self.stack_list[-1]

    def size(self):
        return len(self.stack_list)

    def push(self, value):
        self.stack_list.append(value)

    def pop(self):
        if self.is_empty():
            return None
        else:
            return self.stack_list.pop()

def is_balanced_parentheses(parentheses):
    stack = Stack()
    for p in parentheses:
        if p == '(':
            stack.push(p)
        elif p == ')':
            if stack.is_empty() or stack.pop() != '(':
                return False
    return stack.is_empty()


input_string = "((()))"
print(is_balanced_parentheses(input_string))  # Output: True

input_string = "(()"
print(is_balanced_parentheses(input_string))  # Output: False

input_string = "())("
print(is_balanced_parentheses(input_string))  # Output: False

# COMMAND ----------

# DBTITLE 1,String reverse
def reverse_string2(string):
    stack = Stack()
    reversed_string = ""
 
    for char in string:
        stack.push(char)
 
    while not stack.is_empty():
        reversed_string += stack.pop()
 
    return reversed_string

# COMMAND ----------

# DBTITLE 1,Stack: Sort Stack
class Stack:
    def __init__(self):
        self.stack_list = []

    def print_stack(self):
        for i in range(len(self.stack_list)-1, -1, -1):
            print(self.stack_list[i])

    def is_empty(self):
        return len(self.stack_list) == 0

    def peek(self):
        if self.is_empty():
            return None
        else:
            return self.stack_list[-1]

    def size(self):
        return len(self.stack_list)

    def push(self, value):
        self.stack_list.append(value)

    def pop(self):
        if self.is_empty():
            return None
        else:
            return self.stack_list.pop()




##### WRITE SORT_STACK FUNCTION HERE #####
#                                        #
#  This is a separate function that is   #
#  not a method within the Stack class.  #
#                                        #
#  <- INDENT ALL THE WAY TO THE LEFT <-  #
#                                        #
##########################################




my_stack = Stack()
my_stack.push(3)
my_stack.push(1)
my_stack.push(5)
my_stack.push(4)
my_stack.push(2)

print("Stack before sort_stack():")
my_stack.print_stack()

sort_stack(my_stack)

print("\nStack after sort_stack:")
my_stack.print_stack()



"""
    EXPECTED OUTPUT:
    ----------------
    Stack before sort_stack():
    2
    4
    5
    1
    3

    Stack after sort_stack:
    1
    2
    3
    4
    5

"""

# COMMAND ----------


