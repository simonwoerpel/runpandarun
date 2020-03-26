
all: clean install test

install:
	pip install -e .
	pip install twine coverage nose moto boto3 pytest pytest-cov

test:
	rm -rf ./datastore-testdata
	pytest -s --cov=datastore
	rm -rf ./datastore-testdata

build:
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

