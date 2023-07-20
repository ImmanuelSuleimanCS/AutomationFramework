class AuthenticationManager:
    def __init__(self, properties_file_path):
        self.authenticated = False
        self.properties_file_path = properties_file_path
    
    def authenticate(self, username, password):
        # Read the properties file for authentication data
        auth_data = self._read_properties_file()

        # Check if the provided username and password match the authentication data
        if auth_data.get(username) == password:
            self.authenticated = True
            return True
        else:
            return False

    def is_authenticated(self):
        return self.authenticated
    
    def _read_properties_file(self):
        # Read the properties file and return a dictionary of key-value pairs
        auth_data = {}
        with open(self.properties_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=')
                    auth_data[key.strip()] = value.strip()
        return auth_data
