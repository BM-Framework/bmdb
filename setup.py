from setuptools import setup, find_packages

setup(
    name="bmdb",
    version="1.0",
    author="Marouan Bouchettoy",
    author_email="marouanbouchettoy@gmail.com",
    description="BM Database Framework",
    long_description=open("README.md", encoding="utf-8").read(),
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
        "click==8.3.1",
        "colorama==0.4.6",
        "greenlet==3.3.0",
        "python-dotenv==1.2.1",
        "PyYAML==6.0.3",
        "setuptools==80.9.0",
        "SQLAlchemy==2.0.45",
        "typing_extensions==4.15.0",
    ],
    entry_points={
        'console_scripts': [
            'bmdb = bmdb.cli:main',
        ],
    },
)