from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read().strip()


setup(
    name='runpandarun',
    version='0.1.2',
    description='A simple toolkit for managing data from different sources.',
    long_description=readme(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Topic :: Utilities'
    ],
    url='https://github.com/simonwoerpel/runpandarun',
    author='Simon Wörpel',
    author_email='simon.woerpel@medienrevolte.de',
    license='MIT',
    packages=find_packages(exclude=['runpandarun.tests']),
    entry_points={
        'console_scripts': [
            'runpandarun=runpandarun.cli:main'
        ]
    },
    install_requires=[
        'banal',
        'pyyaml',
        'pandas',
        'requests',
        'awesome-slugify',
        'python-dateutil',
        'google-cloud-storage'
    ],
    zip_safe=False
)
