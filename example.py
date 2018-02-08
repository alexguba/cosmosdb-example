from logzero import logger
import cosmosdb

def main():
    endpoint = "https://localhost:8081/"
    masterkey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
    database = "yourdatabasename"

    db = cosmosdb.CosmosDB.instance(endpoint, masterkey, database)
    if not db:
        raise Exception("CosmosDB: Could not connect DocumentDB")

    collection = db.CreateCollectionIfNotExists("yourcollectionname")   
    if not collection:
        raise Exception("CosmosDB: Could not get collection")

    logger.info("CosmosDB: DocumentDB and collection loaded successfully")

    #define the key / value to use
    key = "new_document_key"
    value = "new_document_value"

    #create the document
    doc = db.CreateDocumentFromKeyValue(collection, key, value)
    if doc:
        logger.info("CosmosDB: Document created successfully.")

    #retrieve the document
    doc = db.GetDocument(collection, key)
    if doc:
        logger.info("CosmosDB: Document retreived successfully.")
    else:
        raise Exception("CosmosDB: Could not retrieve document.")

    #delete document
    db.DeleteDocument(collection, key)
    try:
        doc = db.GetDocument(collection, key)
        logger.info("CosmosDB: Document was not deleted.")
    except Exception as e:
        pass #document is deleted

if __name__ == "__main__":
    main()
