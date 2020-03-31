import redis

class RedisWrapper():

    def __init__(self, host, port):
        self.conn = redis.Redis(host=host, port=6379, db=0, decode_responses=True)
    
    def getConnection(self):
        return self.conn

    def getAll(self):
        '''
            Get all keys from Redis.
            Useful for Files
        '''
        return self.conn.scan_iter("file:*")

    
    def availableNodes(self):
        '''
            Fetch and return available nodes 
        '''
        return self.conn.get('nodes')
    
    def setValue(self, key, value):
        '''
            Set a Key-Value pair 
        '''
        self.conn.set(key, value)

    def fetchValue(self, key):
        '''
            Fetch a Key for given value
        '''
        return self.conn.get(key)

