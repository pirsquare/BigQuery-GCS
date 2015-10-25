from setuptools import find_packages
from setuptools import setup
import io
import os

VERSION = '0.0.4'


def fpath(name):
    return os.path.join(os.path.dirname(__file__), name)


def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(fpath(filename), encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)


def get_requirements():
    return [line.rstrip('\n') for line in open(fpath('requirements.txt'))]


setup_args = dict(
    name='BigQuery-GCS',
    description='Export Large Results from BigQuery to Google Cloud Storage',
    long_description=read('README.md'),
    url='https://github.com/pirsquare/BigQuery-GCS',
    version=VERSION,
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    install_requires=get_requirements(),
    author='Ryan Liao',
    author_email='pirsquare.ryan@gmail.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
)

if __name__ == '__main__':
    setup(**setup_args)
