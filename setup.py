# setup.py
from setuptools import setup, find_packages

setup(
    name="market_state_api",
    version="0.1.0",
    packages=find_packages(),   # will find market_pipeline and scripts if they have __init__.py
)
