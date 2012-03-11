
DOCS_SRC := docs
DOCS := $(DOCS_SRC)/_build/html
BuildDir := build
JAR := $(BuildDir)/seal.jar

# If a VERSION file is available, the version name is taken from there.
# Else a development version number is made from the current timestamp
version := $(shell cat VERSION 2>/dev/null || date "+devel-%Y%m%d") #_%H%M%S")
override_version_check := false

SealName := seal-$(version)
SealBaseDir := $(BuildDir)/$(SealName)
Tarball := $(BuildDir)/$(SealName).tar.gz

doc: $(DOCS)

$(DOCS): $(DOCS_SRC)
	make -C $< html

upload-docs: doc
	rsync -avz --delete -e ssh --exclude=.buildinfo docs/_build/html/ ilveroluca,biodoop-seal@web.sourceforge.net:/home/project-web/biodoop-seal/htdocs

source-dist:
	# make a source archive of the current HEAD
	mkdir --parents $(BuildDir)
	git archive --format=tar --prefix=$(SealName)-src/ HEAD | gzip > $(BuildDir)/$(SealName)-src.tar.gz
