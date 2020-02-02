from setuptools import setup

with open ('README.md', 'r') as fh:
    long_description = fh.read()

setup (
    name="sfbulk2",
    version="0.1.0",
    description="Util Class for Salesforce Bulk API 2.0",
    py_modules=["sfbulk2"],
    package_dir={'':'src'},
    long_description=long_description,
    long_description_content_type="text/markdown",


    url="https://github.com/mohan-chinnappan-n/sfbulk2",
    author="Mohan Chinnappan",
    author_email='mohan.chinnappan.n@gmail.com',


    classifiers= [
        "Programming Language :: Python :: 3.6",
        "License :: MIT"
    ]


)

# ref: https://www.youtube.com/watch?v=QgZ7qv4Cd0Y
# python setup.py bdist_wheel
## installling current dir
# pip install -e .
