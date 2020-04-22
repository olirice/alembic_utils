from setuptools import find_packages, setup

DEV_REQUIRES = [
    "black",
    "pylint",
    "pre-commit",
    "mypy",
    "sqlalchemy-stubs",
    "pytest",
    "pytest-cov",
    "mkdocs",
]

setup(
    name="alembic_utils",
    version="0.1.2",
    author="Oliver Rice",
    author_email="oliver@oliverrice.com",
    license="MIT",
    description="A sqlalchemy/alembic extension for migrating procedures and views ",
    python_requires=">=3.7",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=["alembic", "psycopg2-binary", "flupy", "sqlalchemy", "parse"],
    extras_require={
        "dev": DEV_REQUIRES,
        "nvim": ["neovim", "python-language-server"],
        "docs": ["mkdocs", "pygments", "pymdown-extensions"],
    },
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: SQL",
    ],
)
