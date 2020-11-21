import codecs
import os.path as op
from setuptools import setup

PACKAGE_NAME = "backtest_crypto"

here = op.abspath(op.dirname(__file__))


def read(rel_path):
    with codecs.open(op.join(here, rel_path), 'r') as fp:
        return fp.read()


with open(op.join(here, "README.rst"), encoding="utf-8") as fp:
    README = fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = "="
            return line.split(delim)[-1].strip().strip('"')
    else:
        raise RuntimeError("Unable to find version string.")


VERSION = get_version(op.join(op.dirname(__file__), PACKAGE_NAME, "version.py"))

extras = {
}

setup(
    name=PACKAGE_NAME,
    author="Vikramaditya Gaonkar",
    packages=["backtest_crypto",
              "backtest_crypto.history_collect",
              ],
    url="https://github.com/vikramaditya91/backtest_crypto",
    author_email="vikramaditya91@gmail.com",
    python_requires=">=3.8.0",
    classifiers=[
        "Development Status :: 1 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Topic :: Utilities",
    ],
    description=(
        "backtest_crypto's purpose is to validate the predictions made in the past using the crypto_oversold"
    ),
    extras_require=extras,
    install_requires=[
        'crypto_oversold @ git+ssh://github.com:vikramaditya91/crypto_oversold.git'
        'sqlalchemy',
        'matplotlib'
    ],
    keywords="binance cryptocurrency xarray",
    license="Simplified BSD License",
    long_description=README,
    version=VERSION,
)
