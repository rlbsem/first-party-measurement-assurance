"""SDK staging package; historical root packaging remains unchanged."""

from setuptools import setup

setup(
    name="governed-streaming-measurement",
    version="1.0.0",
    packages=["streaming"],
    package_dir={"streaming": "."},
    package_data={"streaming": ["*.sql"]},
)
