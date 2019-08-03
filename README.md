# Kafka Streams

This project explores the Kafka code base. 

## Input

The input is two streams of data:
1. Classroom Initialization

Classrooms are created in the format of `RoomA,100` where the name is RoomA and the capacity is 100 students.

2. Student Assignment

Students are created in the format of `StudentA,Room1` where the name of the student is StudentA and they are assigned to Room1.

Students can be moved back and forth between classrooms, even classrooms that have not been initialized. Those classrooms have infinite capacity.

## Output

The output is a stream that will output when a room overflows or when a room is no longer overflowing.

The two possible outputs:

1. RoomA,101

This means that RoomA is overflowing and currently has 101 people.

2. RoomA, OK

This means that RoomA is no longer overflowing and is now at capacity. This only shows the first time its not overflowing.
