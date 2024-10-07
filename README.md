# File Provider

File Provider is a flexible file management system that supports both local and S3 storage. It provides an easy-to-use interface for saving, updating, and retrieving files in CSV, Parquet, and JSON formats.

## Setup

This project uses Poetry for dependency management. Follow these steps to set up the project:

1. Install Poetry:
   ```
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. Clone the repository:
   ```
   git clone https://github.com/yourusername/file-provider.git
   cd file-provider
   ```

3. Install dependencies:
   ```
   poetry install
   ```

4. Activate the virtual environment:
   ```
   poetry shell
   ```

## Usage

The `FileProvider` class is the main interface for interacting with files. Here's a basic usage example:
