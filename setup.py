from setuptools import setup, find_packages

setup(
    name="sales-data",
    version="1.0.0",
    description="EternalTeleSales Fran van Seb Group - PySpark Data Pipeline",
    author="Monojit Paul",
    python_requires=">=3.10",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark==3.5.0",
    ],
    extras_require={
        "dev": [
            "chispa==0.9.4",
            "pytest==7.4.4",
            "mypy==1.8.0",
            "black==24.1.1",
            "isort==5.13.2",
            "flake8==7.0.0",
            "pre-commit==3.6.0",
            "pydantic==2.5.3",
        ]
    },
    entry_points={
        "console_scripts": [
            "sales-data=sales_data.main:main",
        ],
    },
)
