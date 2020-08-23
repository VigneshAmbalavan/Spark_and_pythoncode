class test():
     def __init__(self,data=None):
        self.value=data
        #print(self.value)
     @property
     def statement(self):
        self.sentence="the name is %s"%self.value
        return self.sentence
     @statement.setter
     def statement(self,value):
          self.value=value

    # def print(self):
    #     print(self.value)


t=test('agam')
print(t.statement)
######without making the object again changing the vale with use of property decorator#####
t.value='rahul'

print(t.statement)




