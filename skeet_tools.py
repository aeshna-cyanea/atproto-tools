import requests
from bs4 import BeautifulSoup
import re
from typing import Any
from pygrister.api import GristApi


# for now we just grab github repo and discard any file/folder urls.
# TODO: add support for files/folder in repos here and downstream
gh_regex = r"(https://github\.com/[^/]*/[^/]*)/?$"
# more ambitious TODO: add support for other forges
did_regex = r"(did:[a-z0-9]+:(?:(?:[a-zA-Z0-9._-]|%[a-fA-F0-9]{2})*:)*(?:[a-zA-Z0-9._-]|%[a-fA-F0-9]{2})+)(?:[^a-zA-Z0-9._-]|$)"

g = GristApi()

source_name = "Skeet_Tools"

name_field = source_name + "_Name"
desc_field = source_name + "_Description"
tags_field = source_name + "_Tags"
rating_field = source_name + "_Rating"

fields = [name_field, desc_field, tags_field, rating_field]

# fully automated D:
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
    # return print(table_name if use_single_table else single_table_name)
    tool_entries = dict()
    repo_records = dict()
    tags = set()
    page_content: Any = BeautifulSoup(
        requests.get("https://dame.blog/skeet-tools/").text, "html.parser"
    )

    sections = page_content.css.select(".post-body > section")
    for section in sections:
        category = section.h2.string
        featured_entry = category.find("Featured") != -1
        if not featured_entry:
            tags.add(category)

        for list in section.css.select("ul"):
            current_h3 = list.previous_sibling.previous_sibling
            if current_h3 and current_h3.name == "h3":
                current_h3 = current_h3.string
                tags.add(current_h3)
            else:
                current_h3 = None

            for item in list.css.select("li > a"):
                name = item.string
                parts = name.split(":", 1)  # sure hope nobody has a colon in their project name
                tool = {
                    name_field: parts[0].strip(),
                    tags_field: [] if featured_entry else [category],
                    rating_field: 1 if featured_entry else 0,
                }
                if len(parts) == 2:
                    tool[desc_field] = parts[1].strip()
                if current_h3:
                    tool[tags_field].append(current_h3)

                item_url = item["href"]
                if item_url not in tool_entries:
                    tool_entries[item_url] = tool
                else:
                    tool_entries[item_url][tags_field] += tool[tags_field]

                if re.search(gh_regex, item_url):
                    repo_records[item_url] = [item_url]

    tag_key = make_tag_key(tags)
    for k,v in tool_entries.items():
        tool_entries[k][tags_field] = apply_tag_key(v[tags_field], tag_key)

    return {
        "source": source_name,
        "records": tool_entries,
        "columns": fields,
        "repos": repo_records
        # "authors":
    }