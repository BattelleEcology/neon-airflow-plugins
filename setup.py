from setuptools import setup, find_packages

setup(
    name="neon-airflow-plugins",
    version="1.0.0",
    description="Neon plugins for apache airflow 2+",
    url="https://github.com/BattelleEcology/neon-airflow-plugins",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Private :: Do Not Upload",
    ],
    packages=find_packages(include=["airflow_neon_plugins.*"]),
    python_requires=">=3",
)
