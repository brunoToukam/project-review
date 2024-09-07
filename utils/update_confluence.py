import requests
import argparse
import json
import sys

parser = argparse.ArgumentParser(description='Upload file to text on Confluence page.')
parser.add_argument("file_to_upload")
parser.add_argument("page_id")
parser.add_argument("atlassian_token")
args = parser.parse_args()

id = args.page_id  # "516685828"
url = "https://docsacem.atlassian.net/wiki/rest/api/content/{id}".format(id=id)

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Basic ' + args.atlassian_token
}


def update_confluence_page(version, space_key, title):
    with open(args.file_to_upload, "r") as f:
        payload = json.dumps({
            "id": id,
            "type": "page",
            "title": title,
            "space": {
                "key": space_key
            },
            "body": {
                "storage": {
                    "value": "".join(f.readlines()[1:]),
                    "representation": "storage"
                }
            },
            "version": {
                "number": version + 1
            }
        })
    response = requests.request("PUT", url, headers=headers, data=payload)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print("Error: " + str(e))
        sys.exit(1)


def get_current_version_metadata():
    response = requests.request("GET", url, headers=headers, data={}).json()
    return response["version"]["number"], response["space"]["key"], response["title"]


if __name__ == "__main__":
    version, space_key, title = get_current_version_metadata()
    result = update_confluence_page(version, space_key, title)
