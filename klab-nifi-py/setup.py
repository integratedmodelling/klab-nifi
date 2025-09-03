from setuptools import setup, find_packages

setup(
    name="klab-nifi-py",            
    version="0.0.1-beta",                
    author="Arnab Moitra",
    author_email="arnab.moitra@bc3research.org",
    description="Python Based Workflow to Interact with K.LAB Semantic Web, using Apache Nifi",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/integratedmodelling/klab-nifi",  
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: AGPLv3", 
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=[
    ],
)
