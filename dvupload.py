from __future__ import annotations

import hashlib
import http
import json
import mimetypes
import os
import pprint

import requests


class DVUpload:
    def __init__(self, server_url: str, api_token: str):
        self.server_url = server_url
        self.API_TOKEN = api_token

    def upload(self, doi: str, filename: str | os.PathLike[str], description: str | None) -> dict:
        """

        :param doi: dataverse persistent id ( DOI number by default)
        :param filename: file name or pathlike string
        :param description:
        :return:
        """
        file_size = os.stat (filename).st_size

        file_metadata = {"status": "ERROR", "data": {}}
        file_metadata["data"]["categories"] = ["Data"]
        file_metadata["data"]["fileName"] = filename.split (os.sep)[-1]
        file_metadata["data"]["mimeType"] = mimetypes.guess_type (filename)[0]
        if file_metadata["data"]["mimeType"] is None:
            file_metadata["data"]["mimeType"] = ""
        if description is not None:
            file_metadata["data"]["description"] = description
        else:
            file_metadata["data"]["description"] = ""

        try:
            presigned_url = requests.get (url=f'{self.server_url}/api/datasets/:persistentId/uploadurls',
                                          headers={'X-Dataverse-key': self.API_TOKEN},
                                          params={'persistentId': f'doi:{doi}', 'size': file_size})

            if presigned_url.status_code != http.HTTPStatus.OK:
                print (f'ERROR: Bad response from server, status code: {presigned_url.status_code}')
                pprint.pprint(presigned_url.json())
                return file_metadata

            file_metadata["data"]["storageIdentifier"] = presigned_url.json ()["data"]["storageIdentifier"]

            upload_meta = self.__upload (filename=filename, presigned_data=presigned_url.json ()["data"])

            if upload_meta["status"] == "OK":
                file_metadata["data"]["checksum"] = upload_meta["checksum"]
            else:
                return file_metadata

        except requests.exceptions.ConnectionError as cerr:
            print (
                f'ERROR: ConnectionError: server_url: {self.server_url}, API_TOKEN: {self.API_TOKEN}\nTraceback: {cerr.with_traceback ()}')
            return file_metadata

        link_file = requests.post (url=f'{self.server_url}/api/datasets/:persistentId/add',
                                   headers={'X-Dataverse-key': self.API_TOKEN},
                                   params={'persistentId': f'doi:{doi}'},
                                   files={'jsonData': (None, json.dumps (
                                       file_metadata["data"]))})  # this is what it shoudl return, file metadata
        if link_file.status_code == http.HTTPStatus.OK:
            file_metadata["status"] = "OK"
            print (f'INFO: File uploaded successfully')
            return file_metadata

        return file_metadata

    def __upload_multipart(self, filename: str | os.PathLike[str], presigned_data: dict):
        print('DEBUG: running multipart')
        try:
            s3_bucket_urls = presigned_data["urls"]
            part_size = presigned_data["partSize"]

            with open (filename, 'rb') as f:
                e_tags = {}
                md5sum = hashlib.md5 ()
                for part_id, supplied_url in s3_bucket_urls.items ():
                    chunk = f.read (part_size)
                    md5sum.update (chunk)
                    print (f'INFO: uploading part {part_id}')
                    upload_request = requests.put (url=supplied_url, headers={'x-amz-tagging': 'dv-state=temp'},
                                                   data=chunk)
                    e_tags[part_id] = upload_request.headers['ETag'].replace ('\"', '')

            complete_mp_upload = requests.put (url=f'{self.server_url}{presigned_data["complete"]}',
                                               headers={'X-Dataverse-key': self.API_TOKEN},
                                               data=json.dumps (e_tags))

            if complete_mp_upload.status_code != http.HTTPStatus.OK:
                _ = requests.delete (f'{self.server_url}{presigned_data["abort"]}')
                print (
                    f'ERROR: Could not complete multipart upload!\n\tStatus code: {complete_mp_upload.status_code}\n\tResponse: {complete_mp_upload.text}')
                return {"status": "FAILED", "checksum": None}

            print (f'INFO: updating file metadata')

            return {"status": "OK", "checksum": {
                '@type': 'MD5',
                '@value': md5sum.hexdigest ()
            }}

        except KeyError:
            print (f'ERROR: wrong response form S3 storage, please contact DV administrator')
            _ = requests.delete (f'{self.server_url}{presigned_data["abort"]}')
            return {"status": "FAILED", "checksum": None}

        except Exception as e:
            _ = requests.delete (f'{self.server_url}{presigned_data["abort"]}')
            print (f'ERROR: unknown error, this should never happen {e.with_traceback()}')

    def __upload(self, filename: str | os.PathLike[str], presigned_data: dict):
        print("DEBUG: running one part upload")
        if presigned_data.get ("urls") is not None:
            print("DEBUG: this file is to big, running multipart upload")
            return self.__upload_multipart (filename=filename, presigned_data=presigned_data)

        try:
            s3_bucket_urls = presigned_data["url"]
            print("dzy tu jest babol")
            with open (filename, 'rb') as f:
                data_file = f.read ()
                md5sum = hashlib.md5 ()
                md5sum.update (data_file)
                upload_request = requests.put (url=s3_bucket_urls, headers={'x-amz-tagging': 'dv-state=temp'},
                                               data=data_file)
                print (f'Co mi tu zwraca put jak suckess: {upload_request.status_code}')

            print (f'INFO: updating file metadata')
            return {"status": "OK", "checksum": {
                '@type': 'MD5',
                '@value': md5sum.hexdigest ()
            }}

        except KeyError:
            print (f'ERROR: wrong response form S3 storage, please contact DV administrator, {upload_request.request}')
            return {"status": "FAILED", "checksum": None}
        except Exception as e:
            print(f"ERROR: {e.with_traceback()}")
            return {"status": "FAILED", "checksum": None}
