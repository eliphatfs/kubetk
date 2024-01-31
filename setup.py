import setuptools


with open("README.md", "rb") as fh:
    long_description = fh.read().decode()
with open("kubetk/version.py", "r") as fh:
    exec(fh.read())
    __version__: str


def packages():
    return setuptools.find_packages(exclude=['tests'])


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
    install_requires=[
        'numpy', 'typing_extensions'
    ],
    python_requires='~=3.7',
    include_package_data=True,
    package_data={'': ['*.obj', '*.npz', '*.glb']},
)
