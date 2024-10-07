class SQL:
    def __init__(self,id,table,schema,database,connection,materialization,primary_key,inputs,schema_change,code,type):
        self.id = id
        self.table = table
        self.schema = schema
        self.database = database
        self.connection = connection
        self.materialization = materialization
        self.primary_key = primary_key
        self.inputs = inputs
        self.schema_change = schema_change
        self.code = code
        self.type = type