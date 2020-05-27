import fnmatch
import os

dirpath='./logs'

for file in os.listdir('./logs'):
    if fnmatch.fnmatch(file, 'aggregator*.log'):
        print(os.path.abspath(os.path.join(dirpath, file)))
