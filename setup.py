#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-tiktok-ads",
    version="0.2.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_tiktok_ads"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    extras_require={
        "test": [
            "pylint==2.13.4",
            "nose"
        ],
        "dev": [
            "ipdb==0.11"
        ]
    },
    entry_points="""
    [console_scripts]
    tap-tiktok-ads=tap_tiktok_ads:main
    """,
    packages=["tap_tiktok_ads"],
    package_data = {
        "schemas": ["tap_tiktok_ads/schemas/*.json"]
    },
    include_package_data=True,
)
