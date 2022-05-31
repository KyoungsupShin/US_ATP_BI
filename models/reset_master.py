import glob
import os
import sys
from datetime import datetime
sys.path.append('../utils')
from utils import Master_Reset
import threading
import time

if __name__ == '__main__':
    mr = Master_Reset()
    mr.master_reset_main(sender_addr = 'dany.shin@hanwha.com')