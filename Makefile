FILESYSTEM_ENABLED ?= 1
export FILESYSTEM_ENABLED

FILESYSTEM_PUBLISH_ENABLED ?= 1
export FILESYSTEM_PUBLISH_ENABLED

GOOGLE_ENABLED ?= 0
export GOOGLE_ENABLED

GOOGLE_PUBLISH_ENABLED ?= 0
export GOOGLE_PUBLISH_ENABLED

all: clean install test

install:
	pip install -e .
	pip install twine coverage nose moto boto3 pytest pytest-cov

test:
	rm -rf ./datastore-testdata
	pytest -s --cov=runpandarun
	rm -rf ./datastore-testdata

build: readme
	python setup.py sdist bdist_wheel

release: clean build
	twine upload dist/*

clean:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

readme:
	pandoc README.md -o README.rst
	sed -i 's/:panda_face://g' README.rst
