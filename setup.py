from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as fp:
    install_requires = fp.read()

setup(
    name="easyflex",
    version="0.0.13",
    author="Melvin Folkers, Erfan Nariman",
    author_email="melvin@zypp.io, erfan@zypp.io",
    description="Python project voor het ontsluiten van data met de Easyflex API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="python, easyflex",
    url="https://github.com/zypp-io/easyflex",
    packages=find_packages(),
    install_requires=install_requires,
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    project_urls={
        "Bug Reports": "https://github.com/zypp-io/easyflex/issues",
        "Source": "https://github.com/zypp-io/easyflex",
    },
)
