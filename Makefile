
DOCS_SRC := docs
DOCS := $(DOCS_SRC)/_build/html
BuildDir := build
JAR := $(BuildDir)/seal.jar

# If a VERSION file is available, the version name is taken from there.
# Else a development version number is made from the current timestamp
version := $(shell cat VERSION 2>/dev/null || date "+devel-%Y%m%d_%H%M%S")
override_version_check := false

SealName := seal-$(version)
SealBaseDir := $(BuildDir)/$(SealName)
Tarball := $(BuildDir)/$(SealName).tar.gz

.PHONY: clean-doc clean distclean

all: dist

dist: $(Tarball)

$(Tarball): jbuild pbuild
	rm -rf "$(SealBaseDir)"
	mkdir $(SealBaseDir) "$(SealBaseDir)/bin"
	ln $(JAR) $(SealBaseDir)/seal.jar
	ln bin/* $(SealBaseDir)/bin
	cp -r $(BuildDir)/seal $(SealBaseDir)/seal
	cp $(BuildDir)/*.egg-info $(SealBaseDir)/
	tar -C $(BuildDir) -czf $(Tarball) $(SealName)

jbuild: $(JAR)

$(JAR): build.xml src
	ant -Dversion="${version}" -Doverride_version_check=$(override_version_check)

pbuild: seal
	echo version = "'${version}'" > seal/version.py
	python setup.py install --install-lib $(BuildDir) version="${version}" override_version_check=$(override_version_check)

doc: $(DOCS)

$(DOCS): $(DOCS_SRC)
	make -C $< html

upload-docs: doc
	rsync -avz --delete -e ssh --exclude=.buildinfo docs/_build/html/ ilveroluca,biodoop-seal@web.sourceforge.net:/home/project-web/biodoop-seal/htdocs

clean-doc:
	make -C docs clean
	rmdir docs/_build || true

clean: clean-doc
	ant clean
	rm -rf build
	find seal bin -name '*.pyc' -print0 | xargs -0  rm -f
	find seal/lib/seq/aligner/bwa/libbwa/ \( -name '*.ol' -o -name '*.o' -o -name '*.so' \) -print0 | xargs -0  rm -f
	rm -f seal/lib/seq/aligner/bwa/libbwa/bwa
	find . -name '*~' -print0 | xargs -0  rm -f

source-dist:
	# make a source archive of the current HEAD
	mkdir --parents $(BuildDir)
	git archive --format=tar --prefix=$(SealName)-src/ HEAD | gzip > $(BuildDir)/$(SealName)-src.tar.gz

distclean: clean
