class Task:
    def __init__(self,id,schedule,active,steps,force_build,code,type):
        self.id = id
        self.schedule = schedule
        self.active = active
        self.steps = steps
        self.force_build = force_build
        self.code = code
        self.type = type