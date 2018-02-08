from logzero import logger
import pydocumentdb
import pydocumentdb.document_client as document_client

class CosmosDB():
    __instance = None
    _client = None
    _endpoint = None
    _masterkey = None
    _database = None
    _document_db = None

    @staticmethod
    def instance(endpoint, masterkey, database):
        if not CosmosDB.__instance:
            CosmosDB.__instance = CosmosDB(endpoint, masterkey, database)
            _endpoint = endpoint
            _masterkey = masterkey
            _database = database

        return CosmosDB.__instance

    @staticmethod
    def close():
        CosmosDB.__instance = None

    def __init__(self, endpoint, masterkey, database):
        if not endpoint:
            raise Exception("CosmosDB: Endpoint not specified in configuration")  
        if not masterkey:
            raise Exception("CosmosDB: Master Key was not specified in configuration")

        # Initialize the Python DocumentDB client
        self._client = document_client.DocumentClient(endpoint, {'masterKey': masterkey})
        if not self._client:
            raise Exception("Could not create client for endpoint '%s'" % endpoint)

        self._document_db = self.CreateDatabaseIfNotExists(database)
        if not self._document_db:
            raise Exception("Could not connect to DocumentDB '%s'" % database)

        self._database = database
        logger.info("CosmosDB: DocumentDB '%s' connected successfully" % database)

    def CreateCollectionIfNotExists(self, collection):
        document_collection = None
        try:
            document_collection = self._client.ReadCollection(self._GetDocumentCollectionLink({ 'id' : self._database }, { 'id' : collection }))
            logger.info("CosmosDB: DocumentDB: Collection '%s' opened successfully." % collection)
        except Exception as e:
            logger.info("CosmosDB: DocumentDB: Collection could not be opened: %s" % e)

        if not document_collection:
            try:
                logger.info("CosmosDB: DocumentDB: Attempting to create collection '%s'" % collection)
                # Create collection options
                options = {
                    'offerEnableRUPerMinuteThroughput': True,
                    'offerVersion': "V2",
                    'offerThroughput': 400
                }
                # Create a collection
                document_collection = self._client.CreateCollection(self._GetDatabaseLink({ 'id' : self._database }, True),{ 'id': collection }, options)
                logger.info("CosmosDB: DocumentDB: Collection '%s' created successfully." % collection)
            except Exception as e:
                logger.info("CosmosDB: DocumentDB: Collection could not be created: %s" % e)

        return document_collection

    def CreateDatabaseIfNotExists(self, database):
        db = None
        try:
            db = self._client.ReadDatabase(self._GetDatabaseLink({ 'id' : database }, True))
            logger.info("CosmosDB: DocumentDB '%s' opened successfully." % database)
        except Exception as e:
            logger.info("CosmosDB: DocumentDB does not exist or could not be opened: %s" % e)

        #Try and create the database
        if not db:
            try:
                logger.info("CosmosDB: DocumentDB: Attempting to create '%s'" % database)
                db = self._client.CreateDatabase({ 'id': database })
                logger.info("CosmosDB: DocumentDB '%s' created successfully." % database)
            except Exception as e:
                logger.info("CosmosDB: DocumentDB create failed: %s" % e)
    
        return db

    def _GetDatabaseLink(self, database, is_name_based=True):
        if is_name_based:
            return 'dbs/' + database['id']
        else:
            return database['_self']

    def _GetDocumentCollectionLink(self, database, collection, is_name_based=True):
        if is_name_based:
            return self._GetDatabaseLink(database) + '/colls/' + collection['id']
        else:
            return collection['_self']

    def _GetDocumentLink(self, database, collection, document, is_name_based=True):
        if is_name_based:
            return self._GetDocumentCollectionLink(database, collection) + '/docs/' + document['id']
        else:
            return document['_self']

    def GetAllDocuments(self, collection):
        query = { 'query': 'SELECT * FROM c' }    
        options = {} 
        options['enableCrossPartitionQuery'] = True

        result_iterable = self._client.QueryDocuments(collection['_self'], query, options)
        return list(result_iterable)

    def GetDocument(self, collection, key):
        options = {} 
        options['enableCrossPartitionQuery'] = True

        return self._client.ReadDocument(self._GetDocumentLink(self._document_db, collection, { 'id' : key }), options)

    def CreateDocument(self, collection, document_content):
        return self._client.CreateDocument(collection['_self'], document_content)
    
    def CreateDocumentFromKeyValue(self, collection, key, value):
        return self._client.CreateDocument(collection['_self'], { 'id': key, 'value': value })

    def DeleteDocument(self, collection, key):
        return self._client.DeleteDocument(self._GetDocumentLink(self._document_db, collection, { 'id' : key }))
        




    