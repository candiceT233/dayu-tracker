# TODO: this file is not used, for later in a different repo
import setuptools

# TODO: add a readme.md
with open("README.md", "r") as fh:
    long_description = fh.read()

# TODO: add a requirements.txt
with open('requirements.txt') as f:
    required = f.read().splitlines()


setuptools.setup(
    name="flow_analysis",
    version="0.1",
    author="Meng Tang",
    author_email="mtang11@hawk.iit.edu",
    description="Generate data flow analysis graph using Sankey Daigram.",
    # url="https://github.com/candiceT233/vol-tracker/tree/main/flow_analysis", # TODO: make a github repo
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    # install_requires=required,
    python_requires='>=3.6',
    py_modules=['flow_analysis.utils'],  # Add pororo.py as a module
)