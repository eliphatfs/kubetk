import setuptools


with open("README.md", "rb") as fh:
    long_description = fh.read().decode()
with open("kubetk/version.py", "r") as fh:
    exec(fh.read())
    __version__: str


def packages():
    return setuptools.find_packages(include=['kubet*'])


setuptools.setup(
    name="kubetk",
    version=__version__,
    author="flandre.info",
    author_email="flandre@scarletx.cn",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/eliphatfs/kubetk",
    packages=packages(),
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='~=3.7',
    entry_points=dict(
        console_scripts=[
            "kubetk-bulk-zip=kubetk.scripts.bulk_zip:main",
            "kubetk-rmtree=kubetk.scripts.rmtree:main",
            "kubetk-cp=kubetk.scripts.cp:main",
            "kubetk-statsched=kubetk.scripts.statsched:main",
            "statsched=kubetk.scripts.statsched:main",
        ]
    )
)
