import sys
import os
import re
import shutil
import subprocess
from setuptools import setup

if sys.version_info.major == 2:
    assert sys.version_info.minor >= 7
    base_dir = 'py2'
else:
    assert sys.version_info.major >= 3
    if sys.version_info.major == 3 and sys.version_info.minor < 7:
        base_dir = 'py3'
    else:
        base_dir = 'py3.7'

if len(sys.argv) > 1 and sys.argv[1] == 'sdist':
    assert os.path.isdir('py3')
    shutil.rmtree('py3.7', ignore_errors=True)
    shutil.copytree('py3', 'py3.7')
    for path in ['py3.7', os.path.join('py3.7', 'examples')]:
        for filename in os.listdir(path):
            if filename.endswith('.py'):
                filename = os.path.join(path, filename)
                sbuf = os.stat(filename)
                with open(filename, 'r') as fd:
                    lines = [line.replace('raise StopIteration', 'return') for line in fd]
                    for i in range(len(lines)):
                        if lines[i].endswith('sys.version_info.minor < 7, \\\n'):
                            lines[i] = lines[i].replace('sys.version_info.minor < 7',
                                                        'sys.version_info.minor >= 7')
                        else:
                            m = re.match(r'^([\W]*)if sys.version_info >= \(3,7,0\) or ',
                                         lines[i])
                            if m:
                                lines[i] = m.group(1) + 'if True:\n'
                                break
                with open(filename, 'w') as fd:
                    fd.write(''.join(lines))
                os.chmod(filename, sbuf.st_mode)

with open(os.path.join(base_dir, '__init__.py')) as fd:
    regex = re.compile(r'^__version__ = "([\d\.]+)"$')
    for line in fd:
        match = regex.match(line)
        if match:
            module_version = match.group(1)
            break

setup(
    name='pycos',
    version=module_version,
    description='Concurrent, Asynchronous, Distributed, Communicating Tasks with Python',
    long_description=open('README.rst').read(),
    keywords='concurrency, asynchronous, network programming, distributed, tasks, message passing',
    url='https://pycos.org',
    author='Giridhar Pemmasani',
    author_email='pgiri@yahoo.com',
    package_dir={'pycos': base_dir},
    packages=['pycos'],
    setup_requires=['wheel'],
    package_data = {
        'pycos' : ['data/*', 'doc/*', 'examples/*'],
    },
    scripts=[os.path.join(base_dir, script)
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
