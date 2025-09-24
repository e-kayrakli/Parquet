CXX = g++
CHPL_CXX = $(shell $(ARKOUDA_CHPL_HOME)/util/config/compileline --compile-c++ 2>/dev/null)
ifeq ($(CHPL_CXX),)
CHPL_CXX=$(CXX)
endif

INCDIR = include
SRCDIR = src
OBJDIR = obj

CXX_SOURCES = $(wildcard $(SRCDIR)/*.cpp)
CXX_OBJECTS = $(patsubst $(SRCDIR)/%.cpp,$(OBJDIR)/%.o,$(CXX_SOURCES))

CXXFLAGS=-I$(INCDIR)

ARROW_DIR=$(shell spack find --paths --no-groups arrow | cut -d\   -f3)
CXXFLAGS+=-I$(ARROW_DIR)/include

.PHONY: all clean

all: $(CXX_OBJECTS)

printchplflags:
	$(info --ldflags -Wl,-rpath --ldflags $(ARROW_DIR)/lib -L$(ARROW_DIR)/lib -larrow -lparquet)

# Rule to create object directory if it doesn't exist
$(OBJDIR):
	mkdir -p $(OBJDIR)

# Rule to compile C++ source files into object files
$(OBJDIR)/%.o: $(SRCDIR)/%.cpp | $(OBJDIR)
	$(CHPL_CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJDIR) $(BINDIR)


.PHONY: compile-arrow-cpp
compile-arrow-cpp:
	make compile-arrow-write
	make compile-arrow-read
	make compile-arrow-util

.PHONY: compile-arrow-write
compile-arrow-write:
	$(CHPL_CXX) -O3 -std=c++17 -c $(ARROW_WRITE_CPP) -o $(ARROW_WRITE_O) $(INCLUDE_FLAGS)

.PHONY: compile-arrow-read
compile-arrow-read:
	$(CHPL_CXX) -O3 -std=c++17 -c $(ARROW_READ_CPP) -o $(ARROW_READ_O) $(INCLUDE_FLAGS)

.PHONY: compile-arrow-util
compile-arrow-util:
	$(CHPL_CXX) -O3 -std=c++17 -c $(ARROW_UTIL_CPP) -o $(ARROW_UTIL_O) $(INCLUDE_FLAGS)
