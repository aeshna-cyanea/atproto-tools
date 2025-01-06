from pygrister.api import GristApi
import os
from typing import Any
from requests import HTTPError

doc_base_url = "https://docs.getgrist.com/api/docs/t6bKvzR97jxBh6LNmAvLxe/tables/"

# use_single_table = "useSingleTable" in os.environ and os.environ["useSingleTable"] == True
use_single_table = True
single_table_name = os.environ["singleTableName"] if "singleTableName" in os.environ else "Sites"

g = GristApi()

# for use with received data (lists of links to authors/repos indexed by url)
def make_require(entry_dict : dict[str, list], req_key = "URL"):
    flattened = list(set([i for items in entry_dict.values() for i in items]))
    return [{"require": {req_key: v}} for v in flattened]

def put_get_key(table : str, records : list[dict], keyfield = "URL", strip = False):
    if strip:
        records = [{ "require": { keyfield: x["require"][keyfield] }} for x in records]
    g.add_update_records(table, records)
    new_records : list[dict[str, Any]] = g.list_records(table)[1]
    #TODO convert this to a sane sql query based on presence of relevant timestamps (add timestamps first tho)
    return {x[keyfield]: x for x in new_records}


# column metadata is set by hand TODO automate this (column type, width etc)
def make_table_cols(source : str, target_fields: list[str]):
    oldcols = [x["id"] for x in g.list_cols(single_table_name)[1]] # don't like doing this but the put columns api is really funky
    for i in oldcols:
        if i in target_fields:
            target_fields.remove(i)
            if len(target_fields) == 0:
                return

    cols = []        
    for x in target_fields:
        entry : dict[str, Any] = { # numpy refuses to auto recognize the literal's type for some reason
            "id": x,
            "fields": {
                "label": x.replace("_", " ")
            }
        }
        # TODO add more formatting rules. (column type, width etc)
        fields = entry["fields"]
        match x.split("_")[-1]:
            case "Tags":
                # idk what's going on here tbh. trying to set widgetOptions or visibleCol from here goes through 200 ok but sometimes gives weird errors in the grist ui
                fields["type"] = f"RefList:{source + "_Tags"}"
                # fields["widgetOptions"] = "{\"widget\":\"Reference\",\"alignment\":\"left\"}"
                # tag_cols : list[dict] = g.list_cols(source + "_Tags")[1]
                # id_col = next(x for x in tag_cols if x["id"] == "Tag")
                # fields["visibleCol"] = id_col["fields"]["colRef"]
            case "Rating":
                fields["type"] = "Numeric"
            case "Ref": # not used yet, we do it by hand for now
                fields["type"] = f"Ref:{x[:-4]}" # not used yet
            case "Refs":
                fields["type"] = f"RefList:{x[:-5]}"

        cols.append(entry)

    try:
        rcode, resp = g.add_cols(single_table_name, cols)
        #TODO timestamp support. needs to happen after everything else so the trigger formula can bind to the other cols
    except HTTPError:
        raise HTTPError(g.resp_code, g.resp_content)

    return resp

#TODO add support for a single table (with gaps in columns) (and timestamps per data source's group of cols)
def handler(pd: "pipedream"):  # type: ignore  # noqa: F821
    data = pd.steps.gather
    source : str = data["source"]
    columns : list[str] = data["columns"]
    columns.append(source + "_Timestamp")
    # return columns
    make_table_cols(source, columns)
    records : dict[str, dict] = data["records"] 
    
    single_table_key = put_get_key(single_table_name, [{"require": {"URL": url}} for url in records.keys()])     

    if "repos" in data: # the repo urls are real. write them, then get the key to them and save it to the records dict
        repos : dict[str, list[str]] = data["repos"]
        repos_key = put_get_key("Repos", make_require(repos))
        for url, entry_repos in repos.items():
            repos_refs = set([repos_key[x]["id"] for x in entry_repos])
            if "Repos Refs" in single_table_key[url] and isinstance(single_table_key[url]["Repos_Refs"], list):
                repos_refs = repos_refs | set(single_table_key[url]["Repos_Refs"][1:])
            records[url]["Repos_Refs"] = ["L", *repos_refs]
            
    if "authors" in data:
        authors : dict[str, list[str]] = data["authors"]
        authors_key = put_get_key("Authors", make_require(authors, "DID"), "DID")
        for url, entry_authors in authors.items():
            authors_refs = set([authors_key[x]["id"] for x in entry_authors])
            if "Authors_Refs" in single_table_key[url] and isinstance(single_table_key[url]["Authors_Refs"], list):
                authors_refs = authors_refs | set(single_table_key[url]["Authors_Refs"][1:])
            records[url]["Authors_Refs"] = ["L", *authors_refs]

    formatted_records = [{"require": {"URL": url}, "fields": fields} for url, fields in records.items()]
    g.add_update_records(single_table_name, formatted_records)
    print(g.resp_reason, g.resp_content)
    return records