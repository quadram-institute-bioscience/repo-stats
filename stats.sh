#!/bin/bash

# Configuration
ORG="quadram-institute-bioscience"

# Check if token is set
if [[ -z $TOKEN ]]; then
    echo "Error: Please set your GitHub token as \$TOKEN" >&2
    if [[ -e ".env" ]]; then
      echo "Source .env first"
    else
      echo "Make a .env file first"
    fi
    exit 1
echo
   echo "TOKEN for $ORG found" >&2
fi


# Function to make API calls with error handling
make_api_call() {
    local url="$1"
    local response
    response=$(curl -s -w "%{http_code}" -H "Authorization: token $TOKEN" "$url")
    local http_code="${response: -3}"
    local body="${response%???}"
    
    if [ "$http_code" -ne 200 ]; then
        echo "API call failed with code $http_code for $url" >&2
        return 1
    fi
    echo "$body"
}

# Function to safely extract JSON values
safe_jq() {
    local data="$1"
    local query="$2"
    local default="${3:-0}"
    
    if [ -n "$data" ] && [ "$data" != "null" ]; then
        echo "$data" | jq -r "$query // $default"
    else
        echo "$default"
    fi
}

# CSV Header - fast statistics only (no 202 endpoints)
echo "repository,stars,forks,watchers,open_issues,contributors_estimate,size_kb,created_date,updated_date,clones_14d,unique_clones_14d,views_14d,unique_views_14d,total_downloads,primary_language,license,has_wiki,has_pages,archived,default_branch,topics"

# Get all repositories
REPOS_DATA=$(make_api_call "https://api.github.com/orgs/$ORG/repos?per_page=100&sort=updated")

if [ $? -ne 0 ]; then
    echo "Failed to fetch repositories" >&2
    exit 1
fi

# Process each repository
echo "$REPOS_DATA" | jq -r '.[] | "\(.name),\(.stargazers_count),\(.forks_count),\(.watchers_count),\(.open_issues_count),\(.size),\(.created_at),\(.updated_at),\(.language // ""),\(.license.key // ""),\(.has_wiki),\(.has_pages),\(.archived),\(.default_branch),\(.topics | join(";"))"' | \
while IFS=',' read -r REPO STARS FORKS WATCHERS OPEN_ISSUES SIZE CREATED UPDATED LANGUAGE LICENSE HAS_WIKI HAS_PAGES ARCHIVED DEFAULT_BRANCH TOPICS; do
    
    echo "Processing: $REPO" >&2
    
    # Get traffic statistics (clones)
    CLONE_DATA=$(make_api_call "https://api.github.com/repos/$ORG/$REPO/traffic/clones")
    CLONES=$(safe_jq "$CLONE_DATA" '.count')
    UNIQUE_CLONES=$(safe_jq "$CLONE_DATA" '.uniques')
    
    # Get traffic statistics (views)
    VIEW_DATA=$(make_api_call "https://api.github.com/repos/$ORG/$REPO/traffic/views")
    VIEWS=$(safe_jq "$VIEW_DATA" '.count')
    UNIQUE_VIEWS=$(safe_jq "$VIEW_DATA" '.uniques')
    
    # Get release download statistics
    DOWNLOAD_DATA=$(make_api_call "https://api.github.com/repos/$ORG/$REPO/releases")
    DOWNLOADS=$(safe_jq "$DOWNLOAD_DATA" '[.[].assets[]?.download_count] | add')
    
    # Get contributors count (limited to first 30 for speed)
    CONTRIBUTORS_DATA=$(make_api_call "https://api.github.com/repos/$ORG/$REPO/contributors?per_page=30")
    CONTRIBUTORS=$(safe_jq "$CONTRIBUTORS_DATA" 'length')
    if [ "$CONTRIBUTORS" = "30" ]; then
        CONTRIBUTORS="30+"
    fi
    
    # Output CSV row
    echo "$REPO,$STARS,$FORKS,$WATCHERS,$OPEN_ISSUES,$CONTRIBUTORS,$SIZE,$CREATED,$UPDATED,$CLONES,$UNIQUE_CLONES,$VIEWS,$UNIQUE_VIEWS,$DOWNLOADS,$LANGUAGE,$LICENSE,$HAS_WIKI,$HAS_PAGES,$ARCHIVED,$DEFAULT_BRANCH,\"$TOPICS\""
    
    # Small delay to avoid rate limiting
    sleep 0.2
    
done

echo "Statistics collection complete" >&2
