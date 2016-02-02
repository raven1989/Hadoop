#!/usr/bin/env ruby
STDIN.each_line do |line|
  val = line
  year, temperature = val[0,4], val[5,5]
  puts "#{year}\t#{temperature}"
end
