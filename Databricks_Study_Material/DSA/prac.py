# Databricks notebook source
class Node:
  def __init__(self, value):
    self.value=value
    self.next=None

class LinkedList:
  def __init__(self, value):
    newnode=Node(value)
    self.head=newnode
    self.tail=newnode
    self.length=1

  def append(self, value):
    newnode=Node(value)
    if self.length==0:
      self.head=newnode
      self.tail=newnode
      self.length=1    
    else:
      self.tail.next=newnode
      self.tail=newnode
    self.length+=1

  def print_list(self):
    temp=self.head
    while temp:
      print(temp.value)
      temp=temp.next
  
  def pop(self):
    if self.length==0:
      return None
    temp=self.head
    pre=self.head

    while temp.next:
      pre=temp
      temp=temp.next
    
    self.tail=pre
    self.tail.next=None
    self.length-=1

    if self.length==0:
      self.head=None
      self.tail=None
    
  def prepend(self, value):
    newnode=Node(value)
    if self.length==0:
      self.head=newnode
      self.tail=newnode
    else:
      newnode.next=self.head
      self.head=newnode
    self.length+=1
  
  def pop_first(self):
      if self.length==0:
        return None
      else:
        temp=self.head
        self.head=temp.next
        temp=None
      self.length-=1
  
  def get(self, index):
    if index < 0 and index >= self.length:
      return None
    else:
      temp=self.head

      for _ in range(index):
        temp=temp.next
      return temp
  
  def set(self, index, value):
    if index < 0 and index >= self.length:
      return None
    else:
      temp=self.get(index)   
      if temp:
        temp.value=value
        return True
      return False

  def insert(self, index, value):
    if index < 0 or index > self.length:
        return False
    if index == 0:
        return self.prepend(value)
    if index == self.length:
        return self.append(value)
    
    newnode=Node(value)

    temp=self.get(index-1)
    if temp:
       newnode.next=temp.next
       temp.next=newnode
    self.length+=1
      
  def remove(self, index):
    if index < 0 or index > self.length:
      return False
    
    pre=self.get(index-1)
    if pre:
      temp=pre.next
      pre.next=temp.next
      temp.next=None
    self.length-=1


      

    

my_linked_list = LinkedList(1)
my_linked_list.append(2)
my_linked_list.append(3)
my_linked_list.append(4)
my_linked_list.append(7)
my_linked_list.print_list()
print("============================")
# my_linked_list.pop()
# my_linked_list.prepend(1000)
# my_linked_list.pop_first()
print(my_linked_list.set(2, 6666666))
print(my_linked_list.insert(2, 9999999))
print(my_linked_list.remove(2))

my_linked_list.print_list()



# COMMAND ----------


