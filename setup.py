import setuptools
from setuptools.dist import Distribution

class BinaryDistribution(Distribution):
    def has_ext_modules(foo):
        return True

with open("README.md", "r") as f:
    long_description=f.read()

setuptools.setup(name='pydolphindb',
    version='1.0.0',
    install_requires=[
        "numpy",
        "pandas"
    ],
    author='JasonYuchen',
    author_email='jasonyuchen@foxmail.com',
    url='https://github.com/JasonYuchen/pydolphindb',
    license='Pybind11',
    description='A C++ boosted DolphinDB API based on Pybind11',
    long_description=long_description,
    packages=setuptools.find_packages(),
    include_package_data=True,
    platforms=[
        "Windows",
        "Linux",
    ],
    classifiers=[
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    python_requires=">=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*",
    distclass=BinaryDistribution)
