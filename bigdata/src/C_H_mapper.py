#!/usr/bin/env python
"""mapper.py"""
# TEST FILE TO TEST INPUT OUTPUT 

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    print("passed by mapper {}".format(line))
    # split the line into words
'''
    words = line.split()
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print ('%s\t%s' % (word, 1))
'''