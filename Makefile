PROJECT_ROOT = $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

LDFLAGS = -lrdkafka++ -lstdc++
CC = gcc
CFLAGS  = -g -Wall
TARGET = KafkaConsumer
RM= rm -f
all: compile
compile:
	$(CC) $(CFLAGS) -I/usr/include/librdkafka $(TARGET).cpp -o $(TARGET) $(LDFLAGS)
clean:
	$(RM) $(TARGET)


