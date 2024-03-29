# Development quickstart

Clone the repository and set up a virtual environment:

```bash
$ git clone ... .
$ python3 -m venv .env
$ source .env/bin/activate
```

Install dependencies and link in the package for development use:

```bash
$ pip install -e .[dev,test]
$ python setup.py develop
$ git fold -h
...
```

Lint and test the source code:

```bash
$ python setup.py check
$ python setup.py test  # Test only
```

Collect code coverage:

```bash
$ python setup.py coverage
```
