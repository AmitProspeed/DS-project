import redis

class RedisWrapper():

    def __init__(self, host, port):
        self.conn = redis.Redis(host=host, port=6379, db=0)
    
    def getConnection(self):
        return self.conn

    def getAll(self):
        '''
            Get all keys from Redis.
            Useful for Files
        '''
    
    def availableNodes(self):
        '''
            Fetch and return available nodes 
            TODO: Implement Heartbeat and update Redis
        '''
    
    def setValue(self, key, value):
        '''
            Set a Key-Value pair 
        '''

    def fetchValue(self, key):
        '''
            Fetch a Key for given value
        '''

