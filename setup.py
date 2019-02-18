from setuptools import setup, find_packages
import codecs


setup(
    name='lexibase',
    description="An SQLITE3 extension for handling wordlists in LingPy and EDICTOR.",
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    zip_safe=False,
    license="GPL",
    include_package_data=True,
    url='https://github.com/lingpy/lexibase',
    long_description=codecs.open('README.md', 'r', 'utf-8').read(),
    long_description_content_type='text/markdown',
    author='Johann-Mattis List',
    author_email='list@shh.mpg.de',
    keywords='historical linguistics, interface, EDICTOR, LINGPY, SQLITE3',
    python_requires='>=3.5',
    entry_points={
        'console_scripts': ['glottolog=pyglottolog.__main__:main'],
    },
    install_requires=[
        'lingpy',
        'csvw',
        'tqdm',
    ],
    extras_require={
        'dev': ['tox>=2.9', 'flake8', 'pep8-naming', 'wheel', 'twine'],
        'test': ['mock>=2', 'pytest>=3.6', 'pytest-mock', 'pytest-cov'],
    },
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
