import sys
import os
import glob
from setuptools import setup

if sys.version_info.major == 3:
    base_dir = 'py3'
else:
    assert sys.version_info.major == 2
    assert sys.version_info.minor >= 7
    base_dir = 'py2'

setup(
    name='pycos',
    version='4.6.1',
    description='Concurrent, Asynchronous, Distributed, Communicating Tasks with Python',
    long_description=open('README.rst').read(),
    keywords='concurrency, asynchronous, network programming, distributed, tasks, message passing',
    url='http://pycos.sourceforge.io',
    author='Giridhar Pemmasani',
    author_email='pgiri@yahoo.com',
    package_dir={'':base_dir},
    packages=['pycos'],
    package_data = {
        'pycos' : ['data/*', 'doc/*', 'examples/*'],
    },
    scripts=[os.path.join(base_dir, 'pycos', script)
             for script in ['dispycos.py', 'dispycosnode.py']],
    license='Apache 2.0',
    platforms='any',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.1',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development :: Libraries',
        ]
    )
