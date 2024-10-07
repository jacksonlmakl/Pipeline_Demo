class Task:
    def __init__(self,id,schedule,active,steps,force_build,code,type):
        self.id = id
        self.schedule = schedule if schedule else ""
        self.active = True if active=='true' else False
        self.steps = steps.split(',')
        self.force_build = True if force_build=='true' else False
        self.type = type