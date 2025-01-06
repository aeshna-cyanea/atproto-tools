import requests
import re
from typing import Any
from pygrister.api import GristApi
import mistune
get_tree = mistune.create_markdown(renderer=None)

gh_regex = r"(https://github\.com/[^/]*/[^/]*)/?$"

g = GristApi()

source_name = "Notjuliet_Aweome_Atproto"

name_field = source_name + "_Name"
desc_field = source_name + "_Description"
tags_field = source_name + "_Tags"
rating_field = source_name + "_Rating"

fields = [name_field, desc_field, tags_field, rating_field]

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
            raise requests.HTTPError(msg=g.resp_content) #type: ignore

    g.add_update_records(tag_table_name, tags_records)
    new_tags = g.list_records(tag_table_name)[1]
    return {x["Tag"]: x["id"] for x in new_tags}

def apply_tag_key(tags : list, key : dict):
    return ["L", *[key[tag] for tag in tags]]

def handler(pd: "pipedream"):  # type: ignore  # noqa: F821
    entries = dict()
    repos = dict()
    tags = set()
    md : Any = get_tree(
        requests.get("https://raw.githubusercontent.com/notjuliet/awesome-bluesky/refs/heads/main/README.md").text,
    )

    #debug or pprint or https://codebeautify.org/python-formatter-beautifier on md is helpful
    current_h2 : list = []
    current_h3 : list = []
    for node in md:
        if node["type"] == "heading" and node["attrs"]["level"] == 2:
            current_h2 = [node["children"][0]["raw"]]
            tags.add(current_h2[0])
            current_h3 = []
        if node["type"] == "heading" and node["attrs"]["level"] == 3:
            current_h3 = [node["children"][0]["raw"]]
            tags.add(current_h3[0])
        if node["type"] == "list": #sublist
            list_items = node["children"]
            for item in list_items: # list entries
                # item always has a single child block_text which has two children, link and description
                link = item["children"][0]["children"][0]
                url = link["attrs"]["url"]
                entry = {
                    name_field: link["children"][0]["raw"], # raw text of the link
                    desc_field: item["children"][0]["children"][1]["raw"][3:],
                    tags_field: current_h2 + current_h3
                }
                entries[url] = entry
                gh_match = re.search(gh_regex, url)
                if gh_match:
                    repos[url] = [gh_match.group(1)]


    tag_key = make_tag_key(tags)
    for k,v in entries.items():
        entries[k][tags_field] = apply_tag_key(v[tags_field], tag_key)

    return {
        "source": source_name,
        "records": entries,
        "columns": fields,
        "repos": repos
    }