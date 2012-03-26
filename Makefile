
DOCS_SRC := docs
DOCS := $(DOCS_SRC)/_build/html

.PHONY: default clean upload-docs

default:
	@echo "=================================================="
	@echo "run 'python setup.py build' to build Seal"
	@echo "run 'make doc' to compile the documentation"
	@echo "=================================================="

doc: $(DOCS)

$(DOCS): $(DOCS_SRC)
	make -C $< html

upload-docs: doc
	rsync -avz --delete -e ssh --exclude=.buildinfo docs/_build/html/ ilveroluca,biodoop-seal@web.sourceforge.net:/home/project-web/biodoop-seal/htdocs

clean:
	make -C docs clean
