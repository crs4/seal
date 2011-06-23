
DOCS_SRC := docs
DOCS := $(DOCS_SRC)/_build/html
BuildDir := build
JAR := $(BuildDir)/seal.jar
# If a VERSION file is available, the version name is take from there.
# Else a development version number is made from the current timestamp
version := $(shell cat VERSION 2>/dev/null || date "+devel-%Y%m%d_%H%M%S")
Tarball := $(BuildDir)/seal-${version}.tar.gz

.PHONY: clean distclean

all: dist
	
dist: $(Tarball)

$(Tarball): jbuild pbuild
	rm -rf $(BuildDir)/seal
	mkdir $(BuildDir)/seal $(BuildDir)/seal/bin
	ln $(JAR) $(BuildDir)/seal/seal.jar
	ln bin/* $(BuildDir)/seal/bin
	cp -r $(BuildDir)/bl $(BuildDir)/seal/bl
	cp $(BuildDir)/*.egg-info $(BuildDir)/seal/
	tar -C $(BuildDir) -czf $(Tarball) seal

jbuild: $(JAR)

$(JAR): build.xml src
	ant -Dversion="${version}"

pbuild: bl
	python setup.py install --install-lib $(BuildDir) version="${version}"

doc: $(DOCS)

$(DOCS): $(DOCS_SRC)
	make -C $< html

upload-docs: doc
	rsync -avz --delete -e ssh --exclude=.buildinfo docs/_build/html/ ilveroluca,biodoop-seal@web.sourceforge.net:/home/project-web/biodoop-seal/htdocs

clean:
	ant clean
	rm -rf build
	make -C docs clean
	rmdir docs/_build docs/_templates docs/_static || true
	find bl -name '*.pyc' -print0 | xargs -0  rm -f
	find bl/lib/seq/aligner/bwa/libbwa/ -name '*.ol' -o -name '*.o' -print0 | xargs -0  rm -f
	rm -f bl/lib/seq/aligner/bwa/libbwa/bwa
	find . -name '*~' -print0 | xargs -0  rm -f

distclean: clean
