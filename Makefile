# Makefile for Compiling laxen, builder and splitter

# Compiler to use
CXX = g++

# Compiler flags
CXXFLAGS = -Wall -Wextra -std=c++11 -g

# Targets
TARGETS = laxen builder splitter

# Default target
all: $(TARGETS)

# Rule to build laxen
laxen: laxen.o
	$(CXX) $(CXXFLAGS) -o laxen laxen.o

# Rule to build builder
builder: builder.o
	$(CXX) $(CXXFLAGS) -o builder builder.o

# Rule to build splitter
splitter: splitter.o
	$(CXX) $(CXXFLAGS) -o splitter splitter.o

# Pattern rule for compiling .cpp files to .o
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean target to remove compiled binaries and object files
.PHONY: clean
clean:
	rm -f $(TARGETS) *.o
	rm -f fifo_*

clean_fifo:
	rm -f fifo_*

# Optional: Rebuild everything
.PHONY: rebuild
rebuild: clean all


run: all
	./laxen -i inputfile2 -l 10 -m 7 -t 10 -e ExclusionList1.txt -o outfile
	make clean

run2: all
	./laxen -i Republic.txt -l 10 -m 7 -t 20 -e ExclusionList1.txt -o outfile
	make clean

run3: all
	./laxen -i Republic.txt -l 10 -m 7 -t 20 -e ExclusionList2.txt -o outfile
	make clean

run4: all
	./laxen -i GreatExpectations.txt -l 20 -m 17 -t 20 -e ExclusionList1.txt -o outfile
	make clean

run5: all
	./laxen -i WilliamShakespeareWorks.txt -l 20 -m 17 -t 20 -e ExclusionList2.txt -o outfile
	make clean

valgrind: all
	-valgrind --leak-check=full --track-origins=yes ./laxen -i inputfile2 -l 4 -m 7 -t 4 -e ExclusionList1.txt -o outfile
	make clean
	

