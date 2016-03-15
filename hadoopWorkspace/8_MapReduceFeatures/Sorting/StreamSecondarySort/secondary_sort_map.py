#!/usr/bin/env python
import sys

RECORD_LENGTH = 15

def is_valid_record(record):
  return len(record) >= RECORD_LENGTH

for line in sys.stdin:
  val = line.strip()
  if not is_valid_record(val):
    continue
  (year, temp, station) = val.split()[:3]
  if int(temp) != 9999:
    print "%s\t%s" %(year,temp)
