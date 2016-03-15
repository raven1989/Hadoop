#!/usr/bin/env python
#coding:utf-8

import sys
#基本思路参考java版本的Secondary Sort，这个用streaming实现同样的功能
#仍然设定键为组合键，用年分区，不同的是streaming没有划分组(从输入能看出来，streaming的输入是一行一行的，而不是java的一个values[])，
#因为没有分组，所以不用担心按照组合键分组后导致的每组数据是以(year,temp)为key调用一次reduce，从而得到的是每年的所有不同气温;
#而按照year分区已经保证的所有统一年的数据会被同一个reducer调用，在streaming这里，即是这个程序的一次调用。
#所以，需要reducer自己判断年的变化来检测边界

last_group = None
for line in sys.stdin:
  val = line.strip()
  (year, temp) = val.split("\t")
  if last_group==None or last_group!=year:
    last_group = year
    print val
