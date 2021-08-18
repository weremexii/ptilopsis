from ptilopsis import config
from ptilopsis.utils.decorators import async_wrap

import json
from typing import List, Dict, Any
import firebase_admin
from firebase_admin import credentials, firestore

# https://firebase.google.com/docs/firestore?hl=zh-cn

certificate = config.GOOGLE_CERT
if certificate.startswith('{'):
    certificate = json.loads(certificate)
credential = credentials.Certificate(certificate)
if len(firebase_admin._apps) == 0:
    firebase_admin.initialize_app(credential)
db = firestore.client()

class FirestoreDoucment(object):
    def __init__(self, collection_key, document_key):
        self.document = db.collection(str(collection_key)).document(str(document_key))
    
    @async_wrap
    def set(self, data: Dict[str, Any], merge: bool=False):
        '''
        Create or overwrite the document.\n
        When merge is True, data will be merged into the original document.
        '''
        self.document.set(data, merge)

    @async_wrap
    def update(self, data: Dict[str, Any]):
        self.document.update(data)

    @async_wrap
    def update_nested(self, path_to_key: List[str], value):
        '''
        Update the value of a key in a dict in the doucment
        '''
        dot_notation = '.'.join(map(str, path_to_key))
        self.document.update({
            dot_notation: value
        })
    
    @async_wrap
    def array_union(self, key: str, union_list: list):
        self.document.update({
            key: firestore.ArrayUnion(union_list)
        })

    @async_wrap
    def array_remove(self, key: str, union_list: list):
        self.document.update({
            key: firestore.ArrayRemove(union_list)
        })

    @async_wrap
    def get(self):
        return self.document.get().to_dict()

    @async_wrap
    def exists(self):
        return self.document.get().exists
    