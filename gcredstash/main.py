from __future__ import print_function
import argparse
import sys

import googleapiclient.discovery

from config import Config
from keystore import KeyStore
from kms import GoogleKMS

parser = argparse.ArgumentParser(description='A Credential Management Tool using Google Cloud KMS and Datastore')
subparsers = parser.add_subparsers(dest='command',
                                   help='Try commands like "{name} get -h" to get sub command\'s options'.format(
                                       name=sys.argv[0])
                                   )
create_key_ring_parser = subparsers.add_parser('create-keyring',
                                               help="Creates a KeyRing in the given location (e.g. global)")
create_key_ring_parser.add_argument('new_keyring_id', type=str, help='Unique id for new KeyRing creation in location')

create_key_parser = subparsers.add_parser('create-key',
                                          help="Creates a CryptoKey within a KeyRing in the given location")
create_key_parser.add_argument('new_key_id', type=str, help='Unique id for new key creation in the KeyRing')

get_parser = subparsers.add_parser('get', help="Get the cipher from KeyStore and decrypted the credential")
get_parser.add_argument('name', type=str, help='The name of credential')

put_parser = subparsers.add_parser('put', help="Encrypt the credential and put the cipher on KeyStore")
put_parser.add_argument('name', type=str, help='The name of credential')
put_parser.add_argument('plaintext', type=str, help='Text to be encrypted')

get_all_parser = subparsers.add_parser('get-all', help="Get the cipher from KeyStore and decrypted all the credentials")
list_parser = subparsers.add_parser('list')

parser.add_argument(
    '--project-id',
    default=Config.PROJECT_ID, dest='project_id', type=str, help='GCP Project Id')
parser.add_argument(
    '--keystore-rest-api',
    default=Config.KEYSTORE_REST_API, dest='keystore_rest_api', action='store_true',
    help='Uses Datastore REST API (Useful for GAE Standard environment)'
)
parser.add_argument(
    '--location-id',
    default=Config.DEFAULT_LOCATION_ID, dest='location_id', type=str, help='Google Cloud KMS Location Id')
parser.add_argument(
    '--keyring-id',
    default=Config.DEFAULT_KEY_RING_ID, dest='key_ring_id', type=str, help='Google Cloud KMS KeyRing Id')
parser.add_argument(
    '--key-id',
    default=Config.DEFAULT_CRYPTO_KEY_ID, dest='key_id', type=str, help='Google Cloud KMS CryptoKey Id')


def main():
    args = parser.parse_args()
    key_store = KeyStore(args.project_id, rest_api=args.keystore_rest_api)
    kms_client = googleapiclient.discovery.build('cloudkms', 'v1')
    kms = GoogleKMS(kms_client, args.project_id, args.location_id, args.key_ring_id, key_store)

    if args.command == "put":
        kms.put(args.key_id, Config.DEFAULT_DATASTORE_KIND, args.name, args.plaintext)
    elif args.command == "get":
        print(kms.get(args.key_id, Config.DEFAULT_DATASTORE_KIND, args.name))
    elif args.command == "get-all":
        print(kms.get_all(args.key_id, Config.DEFAULT_DATASTORE_KIND))
    elif args.command == "create-keyring":
        print(kms.create_key_ring(kms_client, args.project_id, args.location_id, args.new_keyring_id))
    elif args.command == "create-key":
        print(kms.create_key(kms_client, args.project_id, args.location_id, args.key_ring_id, args.new_key_id))
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
