import base64


class RESTClient(object):
    def __init__(self, creds, project_id):
        self.creds = creds
        self.project_id = project_id
        self.url = 'https://datastore.googleapis.com/v1/projects/' + project_id

    def _get_authed_session(self):
        from google.auth.transport.requests import AuthorizedSession
        return AuthorizedSession(self.creds)

    def _make_request(self, method, url, data=None, headers=None, **kwargs):
        authed_session = self._get_authed_session()
        return authed_session.request(method, url, data, headers, **kwargs)

    def _yield_key_part(self, key):
        for i in xrange(0, len(key), 2):
            yield key[i:i + 2]

    def key(self, *args):
        return args

    def get(self, key):
        if not key or len(key) % 2 != 0:
            raise ValueError('You must specify a valid datastore key')

        path = []
        for part in self._yield_key_part(key):
            path.append({
                'kind': part[0],
                'name': part[-1]
            })

        r = self._make_request('POST', self.url + ':lookup', json={
            'keys': [{
                'path': path
            }]
        })
        r.raise_for_status()

        result = r.json().get('found', [{}])
        assert len(result) == 1
        return result[0].get('entity', {}).get('properties')

    def put(self, entity):
        if not entity or not isinstance(entity, dict):
            raise ValueError('You must specify entity dict')

        r = self._make_request('POST', self.url + ':commit', json={
            'mutations': [{
                'upsert': entity
            }],
            'mode': 'NON_TRANSACTIONAL'
        })
        r.raise_for_status()

    def query(self, kind):
        class Struct(object):
            def __init__(self, **entries):
                self.__dict__.update(entries)

        r = self._make_request('POST', self.url + ':runQuery', json={
            'query': {
                'kind': [{
                    'name': kind
                }]
            }
        })
        r.raise_for_status()

        results = r.json().get('batch', {}).get('entityResults', [])

        def fetch():
            for result in results:
                name = result.get('entity', {}).get('key', {}).get('path', [{}])[0].get('name')
                yield Struct(**{'key': Struct(**{'name': name})})

        r.fetch = fetch
        return r


class KeyStore(object):
    CIPHER_PROPERTY_KEY = 'cipher'

    def __init__(self, project_id=None, namespace=None, rest_api=False):
        self.rest_api = rest_api
        if rest_api:
            import google.auth
            scopes = [
                'https://www.googleapis.com/auth/datastore'
            ]
            creds, project_id = google.auth.default(scopes)
            self.client = RESTClient(creds, project_id)
        else:
            from google.cloud import datastore
            self.client = datastore.Client(project=project_id, namespace=namespace)

    def get(self, kind, name):
        """
        Get value associated with the name from Datastore
        :param kind: Collection name
        :param name: Datastore key
        :return: str
        """
        key = self.client.key(kind, name)

        entity = self.client.get(key)
        if not entity:
            return None
        elif self.rest_api:
            return base64.b64decode(entity.get(KeyStore.CIPHER_PROPERTY_KEY).get('blobValue'))
        return entity.get(KeyStore.CIPHER_PROPERTY_KEY)

    def put(self, kind, name, content):
        """
        Put value on the Datastore
        :param kind: Collection name
        :param name: Datastore key
        :param content: value to store
        :return:
        """
        if self.rest_api:
            entity = {
                'key': {
                    'path': [{
                        'kind': kind,
                        'name': name
                    }]
                },
                'properties': {
                    KeyStore.CIPHER_PROPERTY_KEY: {
                        'excludeFromIndexes': True,
                        'blobValue': base64.b64encode(content)
                    }
                }
            }
        else:
            from google.cloud import datastore
            key = self.client.key(kind, name)
            entity = datastore.Entity(key=key, exclude_from_indexes=(KeyStore.CIPHER_PROPERTY_KEY,))
            entity[KeyStore.CIPHER_PROPERTY_KEY] = content
        return self.client.put(entity)

    def list(self, kind):
        """

        :param kind: Collection name
        :return: list of Datastore keys
        """

        return [c.key.name for c in self.client.query(kind=kind).fetch()]
