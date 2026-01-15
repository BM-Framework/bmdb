from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="bmdb",
    version="0.1.0",
    author="BM-Framework (Marouan Bouchettoy)",
    description="BMDB - Minimal schema manager for SQLAlchemy",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BM-Framework/bmdb",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "click>=8.0.0",
        "PyYAML>=6.0",
        "python-dotenv>=0.19.0",
        "SQLAlchemy>=1.4.0",
        "psycopg2-binary>=2.9.0",
    ],
    entry_points={
        "console_scripts": [
            "bmdb=bmdb.cli:cli",
        ],
    },
    include_package_data=True,
)