from setuptools import setup, find_packages


setup(
    name="dtb",
    version="0.0.2",
    author="Zeta",
    author_email="vince@zeta.ms",
    description="Databricks Tool Box",
    long_description="Utilities for ETL tasks",
    long_description_content_type="text/markdown",
    url="https://github.com/zeta-co/dtb",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
