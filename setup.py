from os.path import join

from hdx.utilities import CleanCommand, PackageCommand, PublishCommand
from hdx.utilities.loader import load_file_to_str
from setuptools import find_namespace_packages, setup


requirements = ["hdx-data-freshness>=1.6.8", "gspread"]

classifiers = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "Natural Language :: English",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Topic :: Software Development :: Libraries :: Python Modules",
]

PublishCommand.version = load_file_to_str(
    join("src", "hdx", "freshness", "emailer", "version.txt"), strip=True
)

setup(
    name="hdx-data-freshness-emailer",
    description="HDX Data Freshness Emailer",
    license="MIT",
    url="https://github.com/OCHA-DAP/hdx-data-freshness-emailer",
    version=PublishCommand.version,
    author="Michael Rans",
    author_email="rans@email.com",
    keywords=["HDX", "fresh", "freshness", "data freshness emailer"],
    long_description=load_file_to_str("README.md"),
    long_description_content_type="text/markdown",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    zip_safe=True,
    classifiers=classifiers,
    install_requires=requirements,
    cmdclass={
        "clean": CleanCommand,
        "package": PackageCommand,
        "publish": PublishCommand,
    },
)
