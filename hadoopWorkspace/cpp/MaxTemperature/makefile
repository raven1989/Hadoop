CC = g++
CPPFLAGS = -m64 -I$(HADOOP_INSTALL)/include

max_temperature: max_temperature.cpp
	# $(CC) $(CPPFLAGS) $< -Wall -L$(HADOOP_INSTALL)/lib/native -L/lib/x86_64-linux-gnu/ -lhadooppipes $(HADOOP_INSTALL)/lib/native/libhadooputils.a -lpthread -lcrypto -g -O2 -o $@
	$(CC) $(CPPFLAGS) $< -Wall -L$(HADOOP_INSTALL)/lib/native -L/lib/x86_64-linux-gnu/ -lhadooppipes -lhadooputils -lpthread -lcrypto -g -O2 -o $@

test:
	echo $(HADOOP_INSTALL)
