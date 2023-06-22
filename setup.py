#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-tiktok-ads",
    version="0.4.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_tiktok_ads"],
    install_requires=[
        "singer-python==5.13.0",
        "requests==2.28.2",
    ],
    extras_require={
        "test": [
            "pylint==2.13.4",
            "nose"
        ],
        "dev": [
            "ipdb"
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
