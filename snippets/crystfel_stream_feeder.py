import sys
from time import sleep

for line in sys.stdin:
    sys.stdout.write(line)
    if line.startswith("----- End chunk"):
        sleep(0.5)
