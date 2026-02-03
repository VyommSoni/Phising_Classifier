#This python scripts we made it cus,we can make our project in this package 
#and we distribute this to anyone...
from setuptools import find_packages,setup
from typing import List

def requires_package(file:str) ->list[str]:
    requirements=[]
    with open(file) as file:
        requirements=file.readlines()
        requirements=[i.replace('\n','') for i in requirements]
    return requirements


setup(
    name='PhisingClasifier',
    author='Vyom',
    author_email='svyom21@gmail.com',
    version='0.0.1',
    install_requires=requires_package('requirements.txt'),
    packages=find_packages()
)