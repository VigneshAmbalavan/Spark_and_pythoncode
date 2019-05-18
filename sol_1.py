
import sys
import csv
import re
import os
from os import listdir
from subprocess import call
from datetime import datetime
import ConfigParser
import argparse
import shutil

##will use python and we get data from the hive table itself using os library, we can use python lib to connect to hive directly also

r=os.systems("beeline  -u %s --showHeader="false" --outputformat="tsv2" -e "select concat(name,'~',age) from X""%(hive_connection_string))

no_dup={}   ###it is a set(eliminates the duplicate)########
for i in r.split(' '):
  no_dup.add('i')


print("now printing back the unique name and age")

for k in no_dup:
  name=k.split('~')[0]
  age=k.split('~')[1]
  print(name,age)