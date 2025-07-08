from collections import deque
from itertools import islice

# suppose this is your set
my_set = {...}  

# 1. size
size = len(my_set)
print(f"Total elements in set: {size}")

# 2. first 100 elements (arbitrary order)
first_100 = list(islice(my_set, 100))
print("First 100 elements:", first_100)

# 3. last 100 elements (arbitrary order)
#    deque with maxlen=100 will retain only the last 100 items seen
last_100 = list(deque(my_set, maxlen=100))
print("Last 100 elements:", last_100)
