import os
from dotenv import load_dotenv

# Load variables from .env file at module import
load_dotenv()

def get_config(key, default=None):
    """
    Reads an environment variable on demand (lazy load).

    Args:
        key (str): The name of the environment variable.
        default: The default value to return if the variable is not found.

    Returns:
        The value of the environment variable or the default value.
    """
    return os.getenv(key, default)
