import os


def load_env_variables():
    # Check if running in GitHub Actions
    if os.getenv('GITHUB_ACTIONS') != 'true':
        # Load environment variables
        from dotenv import load_dotenv
        load_dotenv()
