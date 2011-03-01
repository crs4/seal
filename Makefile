# Use this to make a tarball distribution.
# FIXME: this is a Java app, we should do everything with ant.

APP := PairReadsQSeq
VERSION := $(shell cat VERSION)

DOCS_SRC := docs
DOCS := $(DOCS_SRC)/_build/html
EXPORT_DIR = svn_export
DIST_DIR := $(APP)-$(VERSION)
TARBALL := $(APP)-$(VERSION).tar.gz
JAR := PairReadsQSeq.jar

COPYRIGHT_OWNER := CRS4
NOTICE_TEMPLATE := $(realpath .)/notice_template.txt
COPYRIGHTER = copyrighter -p $(APP) -n $(NOTICE_TEMPLATE) $(COPYRIGHT_OWNER)
# install copyrighter >=0.4.0 from ac-dc/tools/copyrighter


.PHONY: all build doc clean distclean dist

all: dist
build: $(JAR)
dist: $(EXPORT_DIR)/$(TARBALL)
doc: $(DOCS)

$(JAR): build.xml src
	ant

$(DOCS): $(DOCS_SRC)
	make -C $< html

$(EXPORT_DIR)/$(TARBALL): $(JAR) $(DOCS)
	rm -rf $(EXPORT_DIR) && svn export . $(EXPORT_DIR)
	$(COPYRIGHTER) -r $(EXPORT_DIR)
	mkdir -p $(EXPORT_DIR)/$(DIST_DIR)/docs
	mv $(DOCS) $(EXPORT_DIR)/$(DIST_DIR)/docs/
	mv $(JAR) $(EXPORT_DIR)/$(DIST_DIR)/
	cd $(EXPORT_DIR) && mv AUTHORS COPYING VERSION bin build.xml src $(DIST_DIR)/
	cd $(EXPORT_DIR) && tar czf $(TARBALL) $(DIST_DIR)
	rm -rf $(EXPORT_DIR)/$(DIST_DIR)

clean:
	ant clean
	make -C docs clean
	find . -name '*~' -exec rm -f {} \;

distclean: clean
	rm -rf $(EXPORT_DIR)
