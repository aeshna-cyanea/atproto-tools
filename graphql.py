import requests
import time

# Configuration
GITHUB_TOKEN = ''
REPOSITORIES = [
    {'owner': 'owner1', 'name':'repo1'},
    {'owner': 'owner2', 'name':'repo2'},
    # Add more repositories here
]
BATCH_SIZE = 50
SLEEP_TIME = 1  # Time to sleep between batches in seconds

# GraphQL query template
QUERY_TEMPLATE = """
query {{
  rateLimit {{
    remaining
  }}
  {aliases}
}}
"""

REPO_QUERY = """
  repo{index}: repository(owner:"{owner}", name:"{name}") {{
    defaultBranchRef {{
      target {{
       ... on Commit {{
          committedDate
        }}
      }}
    }}
  }}
"""

headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}"
}

def fetch_repo_info(repositories):
    all_data = {}
    for i in range(0, len(repositories), BATCH_SIZE):
        batch = repositories[i:i+BATCH_SIZE]
        aliases = []
        for idx, repo in enumerate(batch):
            alias = REPO_QUERY.format(index=idx, owner=repo['owner'], name=repo['name'])
            aliases.append(alias)
        query = QUERY_TEMPLATE.format(aliases='\n  '.join(aliases))
        response = requests.post('https://api.github.com/graphql', json={'query': query}, headers=headers)
        if response.status_code == 200:
            data = response.json()
            remaining = data['data']['rateLimit']['remaining']
            print(f"Remaining rate limit: {remaining}")
            for idx, repo in enumerate(batch):
                repo_data = data['data'][f'repo{idx}']
                if repo_data and repo_data['defaultBranchRef'] and repo_data['defaultBranchRef']['target']:
                    commit_time = repo_data['defaultBranchRef']['target']['committedDate']
                    all_data[f"{repo['owner']}/{repo['name']}"] = commit_time
                else:
                    all_data[f"{repo['owner']}/{repo['name']}"] = None
        else:
            print(f"Query failed with status code {response.status_code}: {response.text}")
            break
        time.sleep(SLEEP_TIME)
    return all_data

if __name__ == "__main__":
    repo_info = fetch_repo_info(REPOSITORIES)
    for repo, commit_time in repo_info.items():
        print(f"{repo}: {commit_time}")