
VERSION := $(shell cat VERSION)

DOCS_SRC := docs
DOCS := $(DOCS_SRC)/_build/html
BuildDir := build
JAR := $(BuildDir)/seal.jar
Tarball := $(BuildDir)/seal.tar.gz

.PHONY: clean distclean

all: dist
	
dist: $(Tarball)

$(Tarball): jbuild pbuild
	rm -rf $(BuildDir)/seal
	mkdir $(BuildDir)/seal $(BuildDir)/seal/bin
	ln $(JAR) $(BuildDir)/seal/seal.jar
	ln bin/* $(BuildDir)/seal/bin
	cp -r $(shell find $(BuildDir)/lib -name bl) $(BuildDir)/seal/bl
	tar -C $(BuildDir) -czf $(Tarball) seal

jbuild: $(JAR)

$(JAR): build.xml src
	ant

pbuild: bl
	python setup.py install --prefix $(BuildDir)

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
