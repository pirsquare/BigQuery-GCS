from setuptools import find_packages
from setuptools import setup

VERSION = '0.0.4'

setup_args = dict(
    name='BigQuery-GCS',
    description='Export Large Results from BigQuery to Google Cloud Storage',
    url='https://github.com/pirsquare/BigQuery-GCS',
    version=VERSION,
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['bigquery-python>=0.1.1', 'boto'],
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
