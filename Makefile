
PY?=python

all: install

clean:
	$(PY) setup.py clean
	$(RM) -rf build dist __pycache__ spark-executor.egg*

build: clean
	$(PY) setup.py build

dist: clean
	$(PY) setup.py sdist bdist_wheel

install: clean build
	$(PY) setup.py install

