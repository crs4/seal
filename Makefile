
VERSION := $(shell cat VERSION)
PyVersion := $(shell python -c "import sys; print '%d.%d' % sys.version_info[0:2]")

DOCS_SRC := docs
DOCS := $(DOCS_SRC)/_build/html
JAR := build/seal.jar
PythonTar := build/seqal-$(VERSION)-py$(PyVersion).egg

.PHONY: clean distclean

all: jbuild pdist

jbuild: $(JAR)

$(JAR): build.xml src
	ant

pdist: $(PythonTar)

$(PythonTar): bl
	python setup.py build
	python setup.py bdist_egg --dist-dir build

doc: $(DOCS)

$(DOCS): $(DOCS_SRC)
	make -C $< html

clean:
	ant clean
	rm -rf build
	make -C docs clean
	find bl -name '*.pyc' -print0 | xargs -0  rm -f
	find bl/lib/seq/aligner/bwa/libbwa/ -name '*.ol' -print0 | xargs -0  rm -f
	find . -name '*~' -print0 | xargs -0  rm -f

distclean: clean
