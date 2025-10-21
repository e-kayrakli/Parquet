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

# assumes spack-installed arrow
# TODO error check: spack exists? arrow installed?
ARROW_DIR=$(shell spack find --paths --no-groups arrow | cut -d\   -f3)
CXXFLAGS+=-I$(ARROW_DIR)/include

# also add the src directory to be able to include SharedEnums without
# relative path. This path is relative to prereqs/cpp
CXXFLAGS+=-I../../src

.PHONY: all clean

all: $(CXX_OBJECTS)

# Note -I./src is to be able to use SharedEnums.chpl. This path is relative to
# the project root
printchplflags:
	@$(info -I./src/ --ldflags -Wl,-rpath --ldflags $(ARROW_DIR)/lib -L$(ARROW_DIR)/lib -larrow -lparquet)

# Rule to create object directory if it doesn't exist
$(OBJDIR):
	mkdir -p $(OBJDIR)

# Rule to compile C++ source files into object files
$(OBJDIR)/%.o: $(SRCDIR)/%.cpp | $(OBJDIR)
	$(CHPL_CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJDIR) $(BINDIR)
