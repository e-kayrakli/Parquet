CXX = g++
CHPL_CXX = $(shell $(ARKOUDA_CHPL_HOME)/util/config/compileline --compile-c++ 2>/dev/null)
ifeq ($(CHPL_CXX),)
CHPL_CXX=$(CXX)
endif

# trick from https://www.systutorials.com/how-to-get-the-full-path-and-directory-of-a-makefile-itself/#:~:text=ByEric%20Ma%20Mar%2024,Read%20more:
MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MKFILE_DIR := $(dir $(MKFILE_PATH))

INCDIR = $(MKFILE_DIR)/include
SRCDIR = $(MKFILE_DIR)/src
OBJDIR = $(MKFILE_DIR)/obj

CXX_HEADERS = $(wildcard $(INCDIR)/*.h)
CXX_SOURCES = $(wildcard $(SRCDIR)/*.cpp)
CXX_OBJECTS = $(patsubst $(SRCDIR)/%.cpp,$(OBJDIR)/%.o,$(CXX_SOURCES))

CXXFLAGS=-I$(INCDIR)

# assumes spack-installed arrow
# TODO error check: spack exists? arrow installed?
ARROW_DIR=$(shell spack find --format="{prefix}" arrow)
CXXFLAGS+=-I$(ARROW_DIR)/include -std=c++17

# also add the src directory to be able to include SharedEnums without
# relative path. This path is relative to prereqs/cpp
CXXFLAGS+=-I../../src

.PHONY: all clean

all: $(CXX_OBJECTS)

# Note -I./src is to be able to use SharedEnums.chpl. This path is relative to
# the project root
printchplflags:
	@$(info $(CXX_HEADERS) $(CXX_OBJECTS) -I./src/ --ldflags -Wl,-rpath --ldflags $(ARROW_DIR)/lib -L$(ARROW_DIR)/lib -larrow -lparquet)

# Rule to create object directory if it doesn't exist
$(OBJDIR):
	mkdir -p $(OBJDIR)

# Rule to compile C++ source files into object files
$(OBJDIR)/%.o: $(SRCDIR)/%.cpp | $(OBJDIR)
	$(CHPL_CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJDIR)
