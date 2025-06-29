from pymongo import MongoClient
from bson.objectid import ObjectId
import pymongo.errors 

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """                                           

    def __init__(self, username, password):
        # Connection Variables
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 32575
        DB = 'AAC'
        COL = 'animals'
        
        try:
            self.client = MongoClient(f'mongodb://{username}:{password}@{HOST}:{PORT}', serverSelectionTimeoutMS=5000)
            self.client.server_info()  # Forces connection to trigger errors early
            self.database = self.client[DB]
            self.collection = self.database[COL]
            
        except pymongo.errors.OperationFailure:
            raise PermissionError("Invalid username or password.")
        except pymongo.errors.ServerSelectionTimeoutError:
            raise ConnectionError("Cannot connect to MongoDB server.")
        except Exception as e:
            raise Exception(f"Unexpected error: {e}")

    def create(self, data):
        """
        Create method to implement the C in CRUD
        
        Args:
            data (dict): Document to insert into the collection
            
        Returns:
            bool: True if successful insert, False otherwise
            
        Raises:
            ValueError: If data parameter is empty, None, or not a dictionary
            TypeError: If data parameter is not the correct type
        """
        if data is None:
            raise ValueError("Data parameter cannot be None")
        
        if not isinstance(data, dict):
            raise TypeError(f"Data parameter must be a dictionary, got {type(data).__name__}")
        
        if not data:  # Empty dictionary check
            raise ValueError("Data parameter cannot be an empty dictionary")
        
        try:
            result = self.collection.insert_one(data)
            return result.acknowledged  # True if successful
        except pymongo.errors.PyMongoError as e:
            print(f"MongoDB error inserting document: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during create operation: {e}")
            return False
    
    def read(self, query):
        """
        Read method to implement the R in CRUD
        
        Args:
            query (dict): Key/value lookup pair for MongoDB find operation
            
        Returns:
            list: List of documents matching the query, empty list if no matches or error
            
        Raises:
            ValueError: If query parameter is None or empty
            TypeError: If query parameter is not a dictionary
        """
        if query is None:
            raise ValueError("Query parameter cannot be None")
        
        if not isinstance(query, dict):
            raise TypeError(f"Query parameter must be a dictionary, got {type(query).__name__}")
        
        # if not query:
            # raise ValueError("Query parameter cannot be an empty dictionary")
        
        try:
            result = self.collection.find(query)
            return list(result)  # Convert cursor to list and return
        except pymongo.errors.PyMongoError as e:
            print(f"MongoDB error reading documents: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error during read operation: {e}")
            return []
    
    def update(self, query, update_data):
        """
        Update method to implement the U in CRUD
        
        Args:
            query (dict): Key/value lookup pair to find documents to update
            update_data (dict): Key/value pairs for update operation (should include MongoDB update operators)
            
        Returns:
            int: Number of documents modified, 0 if no documents were modified or error occurred
            
        Raises:
            ValueError: If query or update_data parameters are None or empty or do not contain MongoDB update operators
            TypeError: If query or update_data parameters are not dictionaries
        """
        if query is None:
            raise ValueError("Query parameter cannot be None")
        
        if not isinstance(query, dict):
            raise TypeError(f"Query parameter must be a dictionary, got {type(query).__name__}")
        
        if update_data is None:
            raise ValueError("Update data parameter cannot be None")
        
        if not isinstance(update_data, dict):
            raise TypeError(f"Update data parameter must be a dictionary, got {type(update_data).__name__}")
        
        if not update_data:  # Empty dictionary check
            raise ValueError("Update data parameter cannot be an empty dictionary")
        
        # Check if update_data contains at least one MongoDB update operator
        if not any(k.startswith('$') for k in update_data.keys()):
            raise ValueError("Update data must contain at least one MongoDB update operator (e.g., '$set')")
        
        try:
            # Use update_many to update all matching documents
            result = self.collection.update_many(query, update_data)
            return result.modified_count  # Return number of modified documents
        except pymongo.errors.PyMongoError as e:
            print(f"MongoDB error updating documents: {e}")
            return 0
        except Exception as e:
            print(f"Unexpected error during update operation: {e}")
            return 0
    
    def delete(self, query):
        """
        Delete method to implement the D in CRUD
        
        Args:
            query (dict): Key/value lookup pair to find documents to delete
            
        Returns:
            int: Number of documents removed, 0 if no documents were removed or error occurred
            
        Raises:
            ValueError: If query parameter is None or empty
            TypeError: If query parameter is not a dictionary
        """
        if query is None:
            raise ValueError("Query parameter cannot be None")
        
        if not isinstance(query, dict):
            raise TypeError(f"Query parameter must be a dictionary, got {type(query).__name__}")
        
        if not query:  # Empty dictionary check
            raise ValueError("Query parameter cannot be an empty dictionary")
        
        try:
            # Use delete_many to remove all matching documents
            result = self.collection.delete_many(query)
            return result.deleted_count  # Return number of deleted documents
        except pymongo.errors.PyMongoError as e:
            print(f"MongoDB error deleting documents: {e}")
            return 0
        except Exception as e:
            print(f"Unexpected error during delete operation: {e}")
            return 0
            

