CXX = g++ -O2
LIBS = -L/opt/gtest/lib -lgtest -lgtest_main -lpthread
INCS = -I../src -I/opt/gtest/include
OBJDIR = ../obj
OBJS = $(addprefix $(OBJDIR)/, Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o )

unittest: DBFileTest.o
	$(CXX) -o ./bin/unittest $(OBJS) ./obj/DBFileTest.o  $(LIBS) -lfl


DBFileTest.o : ./src/DBFileTest.cc
	$(CXX) $(INCS) -g -c ./src/DBFileTest.cc -o ./obj/DBFileTest.o


clean:
	rm -f ./bin/*
	rm -f ./obj/*

