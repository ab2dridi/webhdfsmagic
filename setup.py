from setuptools import setup, find_packages

setup(
    name="webhdfsmagic",
    version="0.0.1",
    description="IPython magic commands to interact with HDFS via WebHDFS/Knox",
    author="Ab2dridi",
    author_email="a-d13@hotmail.fr",
    packages=find_packages(),
    install_requires=[
        "requests",
        "pandas",
        "traitlets",
        "ipython"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
    python_requires=">=3.6",
)

