# Databricks notebook source
class Node:
    def __init__(self, value):
        self.value = value
        self.next = None
        self.prev = None
        

class DoublyLinkedList:
    def __init__(self, value):
        new_node = Node(value)
        self.head = new_node
        self.tail = new_node
        self.length = 1

    def print_list(self):
        temp = self.head
        while temp is not None:
            print(temp.value)
            temp = temp.next
        
    def append(self, value):
        new_node = Node(value)
        if self.head is None:
            self.head = new_node
            self.tail = new_node
        else:
            self.tail.next = new_node
            new_node.prev = self.tail
            self.tail = new_node
        self.length += 1
        return True

    def pop(self):
        if self.length == 0:
            return None
        temp = self.tail
        if self.length == 1:
            self.head = None
            self.tail = None 
        else:       
            self.tail = self.tail.prev
            self.tail.next = None
            temp.prev = None
        self.length -= 1
        return temp

    def prepend(self, value):
        new_node = Node(value)
        if self.length == 0:
            self.head = new_node
            self.tail = new_node
        else:
            new_node.next = self.head
            self.head.prev = new_node
            self.head = new_node
        self.length += 1
        return True

    def pop_first(self):
        if self.length == 0:
            return None
        temp = self.head
        if self.length == 1:
            self.head = None
            self.tail = None
        else:
            self.head = self.head.next
            self.head.prev = None
            temp.next = None      
        self.length -= 1
        return temp

    def get(self, index):
        if index < 0 or index >= self.length:
            return None
        temp = self.head
        if index < self.length/2:
            for _ in range(index):
                temp = temp.next
        else:
            temp = self.tail
            for _ in range(self.length - 1, index, -1):
                temp = temp.prev  
        return temp
        
    def set_value(self, index, value):
        temp = self.get(index)
        if temp:
            temp.value = value
            return True
        return False
    
    def insert(self, index, value):
        if index < 0 or index > self.length:
            return False
        if index == 0:
            return self.prepend(value)
        if index == self.length:
            return self.append(value)

        new_node = Node(value)
        before = self.get(index - 1)
        after = before.next

        new_node.prev = before
        new_node.next = after
        before.next = new_node
        after.prev = new_node
        
        self.length += 1   
        return True  

    def remove(self, index):
        if index < 0 or index >= self.length:
            return None
        if index == 0:
            return self.pop_first()
        if index == self.length - 1:
            return self.pop()

        temp = self.get(index)
        
        temp.next.prev = temp.prev
        temp.prev.next = temp.next
        temp.next = None
        temp.prev = None

        self.length -= 1
        return temp
    



my_doubly_linked_list = DoublyLinkedList(1)
my_doubly_linked_list.append(2)
my_doubly_linked_list.append(3)
my_doubly_linked_list.append(4)
my_doubly_linked_list.append(5)
my_doubly_linked_list.isLoop()
# print('DLL before remove():')
# my_doubly_linked_list.print_list()

# print('\nRemoved node:')
# print(my_doubly_linked_list.remove(2).value)
# print('DLL after remove() in middle:')
# my_doubly_linked_list.print_list()

# print('\nRemoved node:')
# print(my_doubly_linked_list.remove(0).value)
# print('DLL after remove() of first node:')
# my_doubly_linked_list.print_list()

# print('\nRemoved node:')
# print(my_doubly_linked_list.remove(2).value)
# print('DLL after remove() of last node:')
# my_doubly_linked_list.print_list()


"""
    EXPECTED OUTPUT:
    ----------------
    DLL before remove():
    1
    2
    3
    4
    5

    Removed node:
    3
    DLL after remove() in middle:
    1
    2
    4
    5

    Removed node:
    1
    DLL after remove() of first node:
    2
    4
    5

    Removed node:
    5
    DLL after remove() of last node:
    2
    4

"""


# COMMAND ----------

# DBTITLE 1,swap head & tail
def swap_first_last(self):
  if self.head is None or self.head == self.tail:
      return
  self.head.value, self.tail.value = self.tail.value, self.head.value


# COMMAND ----------

# DBTITLE 1,Reverse DLL
    def reverse(self):
        temp = self.head
        while temp is not None:
            # swap the prev and next pointers of node points to
            temp.prev, temp.next = temp.next, temp.prev
            
            # move to the next node
            temp = temp.prev
            
        # swap the head and tail pointers
        self.head, self.tail = self.tail, self.head

# COMMAND ----------

# DBTITLE 1,Palindrome
def is_palindrome(self):
    # 1. If the length of the doubly linked list is 0 or 1, then 
    # the list is trivially a palindrome. 
    if self.length <= 1:
        return True
    
    # 2. Initialize two pointers: 'forward_node' starting at the head 
    # and 'backward_node' starting at the tail.
    forward_node = self.head
    backward_node = self.tail
    
    # 3. Traverse through the first half of the list. We only need to 
    # check half because we're comparing two nodes at once: one from 
    # the beginning and one from the end.
    for i in range(self.length // 2):
        # 3.1. Compare the values of 'forward_node' and 'backward_node'. 
        # If they're different, the list is not a palindrome.
        if forward_node.value != backward_node.value:
            return False
        
        # 3.2. Move the 'forward_node' one step towards the tail and 
        # the 'backward_node' one step towards the head for the next iteration.
        forward_node = forward_node.next
        backward_node = backward_node.prev
 
    # 4. If we've gone through the first half of the list without 
    # finding any non-matching node values, then the list is a palindrome.
    return True


# COMMAND ----------


