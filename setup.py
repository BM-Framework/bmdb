from setuptools import setup, find_packages

# Read README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="bmdb",
    version="1.0",
    author="Marouan Bouchettoy",
    author_email="marouanbouchettoy@gmail.com",
    description="BM Database Framework - A lightweight database framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BM-Framework/bmdb",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0",
            "black>=23.0",
            "flake8>=6.0",
        ],
        "mysql": [
            "mysql-connector-python>=8.0",
            "pymysql>=1.0",
        ],
        "postgresql": [
            "psycopg2-binary>=2.9",
        ],
    },
    entry_points={
        "console_scripts": [
            "bmdb=bmdb.cli:main",
        ],
    },
    include_package_data=True,
    keywords="database, orm, framework, sql, nosql",
    project_urls={
        "Bug Reports": "https://github.com/BM-Framework/bmdb/issues",
        "Source": "https://github.com/BM-Framework/bmdb",
        "Documentation": "https://github.com/BM-Framework/bmdb/wiki",
    },
)