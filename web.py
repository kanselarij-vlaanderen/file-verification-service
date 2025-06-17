import os
import math
import datetime
from string import Template

from .sudo_query import query as sudo_query
from helpers import logger
from escape_helpers import sparql_escape_datetime

RELATIVE_STORAGE_PATH = os.environ.get("MU_APPLICATION_FILE_STORAGE_PATH", "").rstrip("/")
STORAGE_PATH = f"/share/{RELATIVE_STORAGE_PATH}"

# Ported from https://github.com/mu-semtech/file-service/blob/dd42c51a7344e4f7a3f7fba2e6d40de5d7dd1972/web.rb#L228
def shared_uri_to_path(uri):
    return uri.replace('share://', '/share/')

# Ported from https://github.com/mu-semtech/file-service/blob/dd42c51a7344e4f7a3f7fba2e6d40de5d7dd1972/web.rb#L232
def file_to_shared_uri(file_name):
    if RELATIVE_STORAGE_PATH:
        return f"share://{RELATIVE_STORAGE_PATH}/{file_name}"
    else:
        return f"share://{file_name}"


class FileCache:
    cache_path = "/cache/files"
    last_created_path = "/cache/last_created"
    
    def get_file_uris(self):
        file_uris = []
        last_created = None

        if os.path.isfile(self.last_created_path):
            with open(self.last_created_path, "r") as f:
                last_created = f.readline().strip()
            
            old_file_uris = self._read_file_uris_from_cache()
            new_file_uris, last_created = self._get_file_uris_from_db(last_created)

            file_uris = old_file_uris + new_file_uris
        else:
            file_uris, last_created = self._get_file_uris_from_db()

        self._write_file_uris_to_cache(file_uris, last_created)
        return file_uris
    
    def _read_file_uris_from_cache(self):
        file_uris = []
        if os.path.isfile(self.cache_path):
            with open(self.cache_path, "r") as f:
                file_uris = f.readlines()
        return [uri.strip() for uri in file_uris]

    def _write_file_uris_to_cache(self, file_uris, last_created):
        if len(file_uris):
            with open(self.cache_path, "w") as f:
                for uri in file_uris:
                    f.write(f"{uri}\n")
        
        if last_created:
            with open(self.last_created_path, "w") as f:
                f.write(f"{last_created}\n")

    def _get_file_uris_from_db(self, from_date=None):
        def paginated_query(from_date=None):
            if from_date:
                formats = [
                    "%Y-%m-%dT%H:%M:%S.%fZ", # 2025-03-04T13:46:54.244Z
                    "%Y-%m-%dT%H:%M:%S%z",   # 2007-12-19T10:23:44+01:00 & 2006-01-17T11:35:37Z
                    "%Y-%m-%dT%H:%M:%S",     # 2001-02-20T08:02:49
                ]
                for format in formats:
                    try:
                        from_date = datetime.datetime.strptime(from_date, format)
                        break
                    except ValueError:
                        pass
                else:
                    # No break, we didn't set from_date
                    raise Exception(f"Did not parse from_date, value: {from_date}")
            query_res = sudo_query(Template("""PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT DISTINCT ?file ?created WHERE {
    { SELECT DISTINCT ?file ?created WHERE {
        ?file a nfo:FileDataObject ;
              nie:dataSource ?virtualFile ;
              dct:created ?created .
        $created_filter
    }
    ORDER BY ASC(?created) }
}
LIMIT 100
""").substitute(created_filter=f"FILTER (?created > {sparql_escape_datetime(from_date)})" if from_date else ""))
            last_created = None
            file_uris = []
            bindings = query_res["results"]["bindings"]
            if bindings:
                file_uris = [b["file"]["value"] for b in bindings]
                last_created = bindings[-1]["created"]["value"]
            return file_uris, last_created

        file_uris = []
        last_created = from_date
        while True:
            batch_file_uris, last_created = paginated_query(last_created)
            if len(batch_file_uris):
                file_uris = file_uris + batch_file_uris
            else:
                break
        return file_uris, last_created

def verify_fs_files_in_db(dir="/share"):
    logger.info(f"Listing files in {dir} folder that have no corresponding nfo:FileDataObject in database")
    for entry in os.scandir(dir):
        if entry.is_dir():
            verify_fs_files_in_db(entry)
        else:
            file = entry.path
            logger.debug(f"Querying DB for file {file}")
            ask_res = sudo_query(Template("""PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    ASK {
        $share_file_uri a nfo:FileDataObject .
    }""").substitute(share_file_uri=f"<{file_to_shared_uri(file)}>"))
            if ask_res["boolean"]:
                logger.debug(f"Found file {file_to_shared_uri(file)}")
            else:
                logger.warning(f"Couldn't find db entry for file {file}")

def verify_db_files_in_fs():
    logger.info("Listing nfo:FileDataObject's with a share:// uri that don't exist on disk")
    file_cache = FileCache()
    file_uris = file_cache.get_file_uris()
    for uri in file_uris:
        file_path = shared_uri_to_path(uri)
        if os.path.exists(file_path):
            logger.debug(f"File with uri {uri} present as file in file-system")
        else:
            logger.warning(f"File with uri {uri} not present as file in file-system")

@app.route("/verify-fs")
def verify_fs():
    verify_fs_files_in_db()
    return "Done verifying"

@app.route("/verify-db")
def verify_db():
    verify_db_files_in_fs()
    return "Done verifying"
