import argparse
from dotenv import load_dotenv
from config.mongodb import MongoDB

load_dotenv()


def list_collection(collection_name, limit=10):
    db = MongoDB.get_db()
    collection = db[collection_name]

    total = collection.count_documents({})
    print(f"\n{'='*60}")
    print(f"Collection: {collection_name}")
    print(f"Total de registros: {total}")
    print(f"{'='*60}")

    if total == 0:
        print("Nenhum registro encontrado.")
        return

    docs = collection.find().sort('updated_at', -1).limit(limit)

    for i, doc in enumerate(docs, 1):
        doc['_id'] = str(doc['_id'])
        print(f"\n--- Registro {i} ---")
        for key, value in doc.items():
            print(f"  {key}: {value}")


def main():
    parser = argparse.ArgumentParser(description='Lista registros das collections DOM')
    parser.add_argument('--limit', type=int, default=10, help='Quantidade de registros por collection (default: 10)')
    parser.add_argument('--collection', choices=['domain', 'utm', 'all'], default='all',
                        help='Collection para listar (default: all)')
    args = parser.parse_args()

    collections = {
        'domain': 'DomRevenueByDomain',
        'utm': 'DomRevenueByUtmCampaign',
    }

    if args.collection == 'all':
        for name in collections.values():
            list_collection(name, args.limit)
    else:
        list_collection(collections[args.collection], args.limit)

    MongoDB.close_connection()


if __name__ == '__main__':
    main()
