from setuptools import setup


install_requires = [
    'jsonschema',
    'freenas.utils',
    'paramiko',
    'six',
    'ws4py',
    'wsaccel'
]

setup(
    name='freenas.dispatcher',
    description='FreeNAS dispatcher client library',
    packages=['freenas', 'freenas.dispatcher'],
    namespace_packages=[str('freenas')],
    license='BSD',
    platforms='any',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
    install_requires=install_requires
)
