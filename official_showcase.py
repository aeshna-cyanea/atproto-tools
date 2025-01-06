from urllib.error import HTTPError
import re
import requests
from pygrister.api import GristApi
from collections import defaultdict
import pyjson5

# for now we just grab github repo and discard any file/folder urls.
# TODO: add support for files/folder in repos here and downstream
gh_regex = r"(https://github\.com/[^/]*/[^/]*)/?$"
# more ambitious TODO: add support for other forges
did_regex = r"(did:[a-z0-9]+:(?:(?:[a-zA-Z0-9._-]|%[a-fA-F0-9]{2})*:)*(?:[a-zA-Z0-9._-]|%[a-fA-F0-9]{2})+)(?:[^a-zA-Z0-9._-]|$)"

source_name = "Official_Showcase"

name_field = source_name + "_Name"
desc_field = source_name + "_Description"
tags_field = source_name + "_Tags"
rating_field = source_name + "_Rating"

fields = [name_field, desc_field, tags_field, rating_field]

g = GristApi()

def make_tag_key(tags: set | dict, source_table : str = source_name) -> dict[str, str]:
    tag_table_name = source_table + '_Tags'

    if isinstance(tags, dict):
        # tags have attributes (columns), and those attrs might not be present on all tags. gotta flatten and set-ify the dict to get all unique values
        tag_cols = [{"id": tagname, "fields": {"label": tagname}} for tagname in list(set(["Tag", *[i for items in tags.values() for i in items]]))] # and don't forget to add the main column
        tags_records = [{"require": {"Tag": k}, "fields":  v} for k,v in tags.items()]
    else:
        tag_cols = [{"id": "Tag", "fields": {"label": "Tag"}}]
        tags_records = [{ "require": { "Tag": x }} for x in tags]

    try:
        g.add_update_cols(tag_table_name, tag_cols, noadd=False, noupdate=False)
    except requests.HTTPError:
        if not g.ok:
            g.add_tables([{"id": tag_table_name, "columns": tag_cols}])
        else:
            # idk how errors work
            raise HTTPError(msg=g.resp_content) #type: ignore

    g.add_update_records(tag_table_name, tags_records)
    new_tags = g.list_records(tag_table_name)[1]
    return {x["Tag"]: x["id"] for x in new_tags}

def handler(pd: "pipedream"): #type: ignore  # noqa: F821

    raw_file = requests.get("https://raw.githubusercontent.com/bluesky-social/bsky-docs/refs/heads/main/src/data/users.tsx").text
    raw_entries = re.search(r"User\[\] = (\[\n{.*?\n\])", raw_file, re.S).group(1) # type: ignore
    assert isinstance(raw_entries, str)
    # sample entry for reference
    # title: 'atproto (C++/Qt)',
    # description: 'AT Protocol implementation in C++/Qt',
    # preview: require('./showcase/example-1.png'),
    # website: 'https://github.com/mfnboer/atproto',
    # source: 'https://github.com/mfnboer/atproto',
    # author: 'https://bsky.app/profile/did:plc:qxaugrh7755sxvmxndcvcsgn',
    # tags: ['protocol', 'opensource'],
    raw_entries = "".join([x for x in raw_entries.splitlines() if x.find("require(") == -1]) # strip lines with "require("
    entries = pyjson5.decode(raw_entries)
    tags_string = re.search("export const Tags.*= ({.*?^})", raw_file, re.M + re.S).group(1) #type: ignore
    assert isinstance(tags_string, str)
    raw_tags : dict[str, dict[str, str]] = pyjson5.decode(tags_string)
    del raw_tags["favorite"] # we keep track of this separately
    # mypy thinks the and operator will return str|dict instead of just dict for some reason
    tags = {}
    og_tags_key = {}
    for og_tag, fields in raw_tags.items():
        tags[fields["label"]] = fields
        og_tags_key[og_tag] = fields.pop("label")
    tag_key = make_tag_key(tags)
    entry_records : dict[str, dict] = dict() # a url can only have one thing associated with it
    repos : defaultdict[str, list] = defaultdict(list) # repo can point to many urls
    authors : defaultdict[str, list] = defaultdict(list) # author can contribute to many urls too
    handles_key : dict[str, str] = {} # for caching DID lookups
    for entry in entries:
        if "website" not in entry: # fix your data guys! https://github.com/bluesky-social/bsky-docs/blob/main/src/data/users.tsx#L846
                entry["website"] = entry["source"] 
        
        record = {
            name_field: entry["title"],
            desc_field: entry["description"],
            tags_field: ["L"],
        }

        for tag in entry["tags"]:
                if tag == "favorite":
                    record[rating_field] = 1
                else:
                    record[tags_field].append(tag_key[og_tags_key[tag]])

        url = entry["website"]
        entry_records[url] = record
        
        repo_match = re.search(gh_regex, entry["source"]) if "source" in entry else re.search(gh_regex, entry["website"]) 
        if repo_match:
            repo = repo_match.group(1)
            # entry["source"] = repo # for people who don't bother to fill out the "source" field and also trailing slashes 
            if repo not in repos[url]:
                repos[url].append(repo)


        if "author" in entry and entry["author"]: # sometimes the data gives null? idk
            author : str = entry["author"]
            did_match = re.search(did_regex, author)
            if did_match:
                author = did_match.group(1)
            elif author.startswith("https://bsky.app/profile/"): #TODO support non-bsky apps such as ouranos etc
                if author in handles_key:
                    print(f"resolved cached handle {author} to {handles_key[author]}")
                    author = handles_key[author]
                else:
                    r = requests.get(f"https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle={author[25:]}")
                    if r.ok:
                        did = r.json()["did"]
                        print(f"resolved {author} to {did}")
                        handles_key[author] = did  # for caching DID lookups
                        author = did
                    else:
                        print(f"could not resolve {author}! {r.reason} {r.content!r}")            
            # else:
            #     authors[author] = {"require": {"Generic Webiste": author}} #TODO support non-atproto sites
            
            if author not in authors[url]:
                authors[url].append(author)
            

    return {
        "records": entry_records, # {url: {columns}, ...} 
        "repos": repos, # {url: [repo_urls], ...}
        "authors": authors, # {url: [author_dids], ...}
        "source": source_name, # str
        "columns": [name_field, desc_field, tags_field, rating_field],
    }