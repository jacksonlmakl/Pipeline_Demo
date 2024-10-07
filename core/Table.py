class Table:
    def __init__(self,id,table,schema,database,connection,materialization,primary_key,inputs,schema_change,code,type):
        self.id = id
        self.table = table
        self.schema = schema
        self.database = database
        self.connection = connection
        self.materialization = materialization if materialization else None
        
        self.primary_key = primary_key if primary_key else None
        self.inputs = inputs if inputs else []
        self.schema_change = schema_change
        self.code = code
        self.type = type
        self.validate()
    def validate(self):
        if self.materialization=='incremental' and self.primary_key==None:
            raise Exception("Incremental materialization requires a valid primary_key argument")
