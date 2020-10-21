from distutils.core import setup
from setuptools import find_packages


with open('README.md') as f:
    readme = f.read()

setup(
    name='hyper_kernel',
    version='0.0',
    packages=find_packages(),
    description='Hyper kernel for Jupyter',
    long_description=readme,
    author='Adrian Vogelsgesang',
    author_email='avogelsgesang@tableau.com',
    package_data={'hyper_kernel': ['data/*.png']},
    install_requires=[
        'jupyter_client', 'IPython', 'ipykernel', 'tabulate'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
    ],
)
