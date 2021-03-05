#!/usr/bin/env python3
#-*- coding: utf-8 -*-

# minimal setup.py script showing how multiple wheels are generated
# https://github.com/pypa/pip/issues/8201#issuecomment-625215137

import setuptools
import subprocess
import sys
import shutil

"""
Setuptools based test script to build .whl python packages (wheel)

Usage: python3 setup.py bdist_wheel
"""


def build_wsc_producer(version: str):
    # remove any previous setuptools build dir
    shutil.rmtree("build", ignore_errors=True)

    print("Starting setuptools.setup() for package wsc-producer")
    setuptools.setup(
        name="wsc-producer",
        version=version,
        description="Python wheel package wsc producer",
        packages=setuptools.find_packages(include=["wsc_producer", "util"], exclude=["wsc_consumer"]),

        install_requires=[
            # "util==%s" % version,
            "kafka-python",
            "psycopg2-binary",
            "aiohttp",
        ],

        entry_points={
            'console_scripts': [
                "wsc_producer=wsc_producer.producer:main",
            ],
        }
    )
    print("Finished setuptools.setup() for package wsc-producer")


def build_wsc_consumer(version: str):
    # remove previous setuptools build dir
    shutil.rmtree("build", ignore_errors=True)

    # Add a separate package for the applications, so we can easily pull in dependencies that are
    # not required for the bindings themselves (e.g. ipython).
    print("Starting setuptools.setup() for package wsc-consumer")
    setuptools.setup(
        name="wsc-consumer",
        version=version,
        description="Python wheel package wsc consumer",
        packages=setuptools.find_packages(include=["wsc_consumer", "util"], exclude=["wsc_producer"]),

        install_requires=[
            # "util==%s" % version,
            "kafka-python",
            "psycopg2-binary",
        ],

        entry_points={
            'console_scripts': [
                "wsc_consumer=wsc_consumer.consumer:main",
            ],
        }
    )
    print("Finished setuptools.setup() for package wsc-consumer")


def main():
    # The actual package version ("public version identifier" in PEP 440). Should be of form x.y.z (etc.)
    public_version = "0.0.1"

    # The "local version identifier" as specified in PEP 440
    # local_version = None

    # See PEP 440 for details on supported version schemes in python packages.
    # -> https://www.python.org/dev/peps/pep-0440/
    # Essentially: they may consist of a "public version" and a "local version" component. The public version must
    # follow a somewhat simple format ("greater" comparison must be possible), so git hashes are not possible.
    # a local version is merely a label.
    version = public_version

    if '--build_package' in sys.argv:
        idx = sys.argv.index('--build_package')
        if len(sys.argv) <= idx + 1:
            print("ERROR: Require argument to --build_package")
            sys.exit(1)
        build_package = sys.argv[idx + 1]
        del sys.argv[idx]
        del sys.argv[idx]
        if build_package == 'wsc-consumer':
            build_wsc_consumer(version=version)
        elif build_package == 'wsc-producer':
            build_wsc_producer(version=version)
        else:
            print("ERROR: Invalid package %s" % build_package)
            sys.exit(1)
    else:
        for package in ['wsc-consumer', 'wsc-producer']:
            argv = sys.argv
            if argv[0] == '-c':
                argv[0] = __file__
            args = [sys.executable] + argv + ['--build_package', package]
            p = subprocess.Popen(args)
            p.communicate()
            if p.returncode != 0:
                print("ERROR: Failed to build package %s" % package)
                sys.exit(p.returncode)
        print("Finished building packages.")


if __name__ == '__main__':
    main()
