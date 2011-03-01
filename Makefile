# Use this to make a tarball distribution.
# FIXME: this is a Java app, we should do everything with ant.

APP := PairReadsQSeq
VERSION := $(shell cat VERSION)

DOCS_SRC := docs
DOCS := $(DOCS_SRC)/_build/html
EXPORT_DIR = svn_export
TARBALL_DIR := $(APP)-$(VERSION)
TARBALL := $(APP)-$(VERSION).tar.gz
JAR := PairReadsQSeq.jar

COPYRIGHT_OWNER := CRS4
NOTICE_TEMPLATE := $(realpath .)/notice_template.txt
COPYRIGHTER = copyrighter -p $(APP) -n $(NOTICE_TEMPLATE) $(COPYRIGHT_OWNER)
# install copyrighter >=0.4.0 from ac-dc/tools/copyrighter


.PHONY: all build doc clean distclean sdist

all: sdist
build: dist/$(JAR)
sdist: dist/$(TARBALL)
doc: $(DOCS)

dist/$(JAR): build.xml src
	ant

$(DOCS): $(DOCS_SRC)
	make -C $< html

dist/$(TARBALL): dist/$(JAR) $(DOCS)
	mkdir -p dist/$(TARBALL_DIR)/docs
	mv $(DOCS) dist/$(TARBALL_DIR)/docs/
	rm -rf $(EXPORT_DIR) && svn export . $(EXPORT_DIR)
	$(COPYRIGHTER) -r $(EXPORT_DIR)
	mv $(EXPORT_DIR)/src $(EXPORT_DIR)/build.xml dist/$(TARBALL_DIR)
	cp AUTHORS COPYING VERSION dist/$(TARBALL_DIR)
	mv dist/$(JAR) dist/$(TARBALL_DIR)
	cd dist && tar czf $(TARBALL) $(TARBALL_DIR)
	rm -rf dist/$(TARBALL_DIR)

clean:
	rm -rf classes
	find . -name '*~' -exec rm -f {} \;
	make -C docs clean
	rm -rf $(EXPORT_DIR)

distclean: clean
	rm -rf dist
