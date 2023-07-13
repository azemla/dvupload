from __future__ import annotations

import hashlib
import http
import json
import mimetypes
import os
import pprint

import requests


class DVUpload:
    MAX_PART_SIZE = 1024 ** 3

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

        if file_size > self.MAX_PART_SIZE:
            file_metadata = self.__upload_multipart (doi, filename, file_size)
        else:
            file_metadata = self.__upload (doi, filename, file_size)

        if file_metadata["status"] != "OK":
            return file_metadata

        if description is not None:
            file_metadata["data"]["description"] = description
        else:
            file_metadata["data"]["description"] = ""

        file_metadata["data"]["categories"] = ["Data"]
        file_metadata["data"]["fileName"] = filename.split (os.sep)[-1]
        file_metadata["data"]["mimeType"] = mimetypes.guess_type (filename)[0]
        if file_metadata["data"]["mimeType"] is None:
            file_metadata["data"]["mimeType"] = ""

        link_file = requests.post (url=f'{self.server_url}/api/datasets/:persistentId/add',
                                   headers={'X-Dataverse-key': self.API_TOKEN},
                                   params={'persistentId': f'doi:{doi}'},
                                   files={'jsonData': (None, json.dumps (file_metadata["data"]))})
        if link_file.status_code == http.HTTPStatus.OK:
            print (f'INFO: File uploaded successfully')
            return file_metadata

        file_metadata["status"] = "FAILED"
        return file_metadata

    def __upload_multipart(self, doi: str, filename: str | os.PathLike[str], file_size: int):
        file_metadata = {'status': 'OK', 'data': {}}
        try:
            presigned_url = requests.get (url=f'{self.server_url}/api/datasets/:persistentId/uploadurls',
                                          headers={'X-Dataverse-key': self.API_TOKEN},
                                          params={'persistentId': f'doi:{doi}', 'size': file_size})
            presigned_url.raise_for_status ()
        except requests.exceptions.HTTPError as err:
            print (
                f'ERROR: couldn\'t get upload url, status code: {err.response.status_code}\n\t Request response {err.response.text}')
            return file_metadata

        try:
            s3_bucket_urls = presigned_url.json ()["data"]["urls"]
            part_size = presigned_url.json ()["data"]["partSize"]

            with open (filename, 'rb') as f:
                e_tags = {}
                md5sum = hashlib.md5 ()
                for part_id, supplied_url in s3_bucket_urls.items ():
                    chunk = f.read (part_size)
                    md5sum.update (chunk)
                    print (f'INFO: uploading part {part_id}')
                    upload_request = requests.put (url=supplied_url, headers={'x-amz-tagging': 'dv-state=temp'},
                                                   data=chunk)
                    upload_request.raise_for_status ()

                    pprint.pprint (upload_request.headers)
                    e_tags[part_id] = upload_request.headers['ETag'].replace ('\"', '')

            pprint.pprint (e_tags)
            complete_mp_upload = requests.put (url=f'{self.server_url}{presigned_url.json ()["data"]["complete"]}',
                                               headers={'X-Dataverse-key': self.API_TOKEN},
                                               data=json.dumps (e_tags))

            if complete_mp_upload.status_code != 200:
                _ = requests.delete (f'{self.server_url}{presigned_url.json ()["data"]["abort"]}')
                print (
                    f'ERROR: Coudn\'t complete multipart upload!\n\tStatus code: {complete_mp_upload.status_code}\n\tResponse: {complete_mp_upload.text}')
                file_metadata["status"] = "FAILED"
                return file_metadata

            print (f'INFO: updating file metadata')
            file_metadata["data"]["storageIdentifier"] = presigned_url.json ()["data"]["storageIdentifier"]
            file_metadata["data"]["checksum"] = {
                '@type': 'MD5',
                '@value': md5sum.hexdigest ()
            }

            return file_metadata

        except requests.exceptions.HTTPError as err:
            print (
                f'ERROR: multipart uploading failed.\n\tStatus code: {err.response.status_code}\n\tRequest response {err.response.text}')
            _ = requests.delete (f'{self.server_url}{presigned_url.json ()["data"]["abort"]}')
            return file_metadata

        except KeyError as kerr:
            print (f'ERROR: wrong response form S3 storage, please contact DV administrator')
            _ = requests.delete (f'{self.server_url}{presigned_url.json ()["data"]["abort"]}')
            return file_metadata

        except Exception as e:
            _ = requests.delete (f'{self.server_url}{presigned_url.json ()["data"]["abort"]}')
            print (f'ERROR: unknown error, this should never happen')
            raise e

    def __upload(self, doi: str, filename: str | os.PathLike[str], file_size: int):
        pprint.pprint(f'lazy')
        file_metadata = {'status': 'OK', 'data': {}}
        return file_metadata
